package com.music.bitchord.data.sources

import com.music.bitchord.data.Http
import com.music.bitchord.data.TrackLog
import java.util.Collections
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import kotlin.coroutines.coroutineContext

/**
 * The flacdownloader.com backend behind the Spotube FLAC plugin, spoken
 * directly: Qobuz catalogue search, a tokened queue for conversions, and a
 * job poll that ends in a file URL. No plugin runtime, no Spotube — the
 * plugin's Hetu scripts reduce to these six calls, and every one of them is
 * plain HTTP and JSON.
 *
 * Two things about this backend shape the client. First, search is open and
 * fast while stream issuance is gated: `/prepare` answers a token or a
 * `challenge_failed`, and without the token there is nothing to queue. A
 * challenge is environmental (bot defense vs. this network), not a miss, so
 * it fails loudly as null-with-a-log-line rather than burning the poll
 * budget proving it. Second, a conversion is a *job*, not a URL: the poll
 * loop is bounded in both count and wall time, delayed between rounds (the
 * plugin hammers with no delay at all), and cancellable at every await, so
 * a slow backend loses the resolver's race instead of holding it.
 */
object FlacDownloaderApi {

    const val BASE = "https://flacdownloader.com"
    private const val TAG = "BitChord"

    /**
     * Test hook: point the client at a MockWebServer instead of the live
     * backend. Internal so production code cannot wander off-host; unit
     * tests reset it after each case.
     */
    internal var baseOverride: String? = null
    private val base: String get() = baseOverride?.trimEnd('/') ?: BASE

    /** Qobuz format ids, best first — the plugin's `[27, 7, 6, 5]`. */
    const val F_24_192 = 27
    const val F_24_96 = 7
    const val F_16_44 = 6
    const val F_MP3_320 = 5

    val LOSSLESS_FORMATS = listOf(F_24_192, F_24_96, F_16_44)
    val LOSSY_FORMATS = listOf(F_MP3_320)

    /** Wall clock for one resolve across all formats: slow backends lose the race. */
    const val RESOLVE_BUDGET_MS = 75_000L
    private const val POLL_DELAY_MS = 2_000L
    private const val MAX_POLLS_PER_FORMAT = 20

    private val json = Json {
        isLenient = true
        ignoreUnknownKeys = true
        explicitNulls = false
        coerceInputValues = true
    }

    /**
     * When a token was last refused by the backend's challenge, until which
     * no new attempt is made. A challenged network answers every attempt
     * identically, so retrying per track would hold each upgrade walk for
     * the full retry budget accomplishing nothing — the miss has to cost
     * milliseconds, not seconds. A success clears it; an explicit user
     * Test bypasses it (see [prepareToken]).
     */
    @Volatile
    private var tokenBlockedUntilMs = 0L
    private const val TOKEN_BLOCK_MS = 10 * 60 * 1000L
    private const val TOKEN_ATTEMPTS = 3
    private const val TOKEN_RETRY_DELAY_MS = 1_500L

    private val client: OkHttpClient by lazy {
        Http.client.newBuilder()
            .callTimeout(20, TimeUnit.SECONDS)
            .connectTimeout(8, TimeUnit.SECONDS)
            .build()
    }

    // ── Wire shapes ───────────────────────────────────────────────────

    @Serializable
    data class QobuzSearchResponse(
        @SerialName("tracks") val tracks: List<QobuzTrack> = emptyList(),
    )

    @Serializable
    data class QobuzTrack(
        @SerialName("id") val id: Long = 0,
        @SerialName("title") val title: String = "",
        @SerialName("artist") val artist: String = "",
        @SerialName("album") val album: String = "",
        @SerialName("cover") val cover: String? = null,
        @SerialName("durationMs") val durationMs: Long? = null,
        @SerialName("isrc") val isrc: String? = null,
        @SerialName("hires") val hires: Boolean = false,
        @SerialName("bitDepth") val bitDepth: Double? = null,
        @SerialName("samplingRate") val samplingRate: Double? = null,
        @SerialName("url") val url: String? = null,
        @SerialName("explicit") val explicit: Boolean = false,
    )

    @Serializable
    private data class TokenResponse(@SerialName("t") val token: String? = null)

    @Serializable
    private data class QueueResponse(@SerialName("id") val jobId: String? = null)

    @Serializable
    private data class JobResponse(
        @SerialName("status") val status: String? = null,
        @SerialName("file") val file: String? = null,
        @SerialName("pretty_name") val prettyName: String? = null,
    )

    @Serializable
    private data class DeezerSearchResponse(
        @SerialName("data") val data: List<DeezerRow> = emptyList(),
    )

    @Serializable
    data class DeezerRow(
        @SerialName("title") val title: String = "",
        @SerialName("isrc") val isrc: String? = null,
    )

    /**
     * What a search row needs later at queue time, parked by packed id.
     *
     * Lives here rather than on any source instance because instances are
     * rebuilt whenever configs change: a memo on the instance strands
     * every row it minted the moment the registry rebuilds, and the stream
     * call then misses on tracks just found. Qobuz ids are global, so one
     * bounded map serves all instances.
     */
    data class Origin(
        val qobuzUrl: String,
        val title: String,
        val artist: String,
        val durationSec: Int?,
    )

    private val origins = Collections.synchronizedMap(
        object : LinkedHashMap<String, Origin>(64, 0.75f, true) {
            override fun removeEldestEntry(eldest: Map.Entry<String, Origin>) = size > 128
        },
    )

    fun memoizeOrigin(packed: String, origin: Origin) {
        origins[packed] = origin
    }

    fun originOf(packed: String): Origin? = origins[packed]

    /** A converted file waiting on the backend's CDN. */
    data class ResolvedFile(
        val url: String,
        val formatId: Int,
    ) {
        val isLossless: Boolean get() = formatId != F_MP3_320
        val sampleRateHz: Int get() = if (formatId == F_24_192) 192_000 else 96_000.takeIf { formatId == F_24_96 } ?: 44_100
        val bitDepth: Int get() = if (formatId == F_24_192 || formatId == F_24_96) 24 else 16
        val kbps: Int get() = if (formatId == F_MP3_320) 320 else 1_411
    }

    // ── Catalogue ─────────────────────────────────────────────────────

    /**
     * Whether the catalogue answers at all. Unlike [searchQobuz], which
     * swallows failure into an empty list, this throws — health needs the
     * distinction between "reachable but holding nothing" and "gone".
     */
    suspend fun probe() {
        get("$base/api/qobuz/search?q=music&offset=0&limit=1")
    }

    suspend fun searchQobuz(query: String, limit: Int = 25): List<QobuzTrack> {
        val q = query.trim().takeIf { it.isNotEmpty() } ?: return emptyList()
        val url = "${base}/api/qobuz/search?q=${encode(q)}&offset=0&limit=${limit.coerceIn(1, 100)}"
        return try {
            json.decodeFromString<QobuzSearchResponse>(get(url)).tracks
        } catch (e: Exception) {
            coroutineContext.ensureActive()
            TrackLog.d(TAG, "  ✗ hifiresolver qobuz search failed: ${e.message}")
            emptyList()
        }
    }

    /** Deezer rows for a query, for the ISRC precision re-search below. */
    suspend fun deezerRows(query: String): List<DeezerRow> {
        val q = query.trim().takeIf { it.isNotEmpty() } ?: return emptyList()
        return try {
            val request = Request.Builder()
                .url("https://api.deezer.com/search?q=${encode(q)}&limit=20")
                .header("Accept", "application/json")
                .build()
            val body = client.newCall(request).execute().use { response ->
                if (!response.isSuccessful) return emptyList()
                response.body?.string()?.takeIf { it.isNotBlank() } ?: return emptyList()
            }
            json.decodeFromString<DeezerSearchResponse>(body).data
        } catch (e: Exception) {
            coroutineContext.ensureActive()
            TrackLog.d(TAG, "  ✗ hifiresolver deezer search failed: ${e.message}")
            emptyList()
        }
    }

    // ── Stream issuance ───────────────────────────────────────────────

    /**
     * A conversion token, or null when the backend's challenge refuses this
     * network. Null is environmental and worth logging once: every queue
     * attempt without it fails identically, so callers check this first.
     */
    suspend fun prepareToken(force: Boolean = false): String? {
        if (!force && System.currentTimeMillis() < tokenBlockedUntilMs) return null
        repeat(TOKEN_ATTEMPTS) { attempt ->
            try {
                val token = json.decodeFromString<TokenResponse>(get("${base}/prepare"))
                    .token?.takeIf { it.isNotBlank() }
                if (token != null) {
                    tokenBlockedUntilMs = 0L
                    return token
                }
                TrackLog.w(TAG, "  ✗ hifiresolver prepare refused (challenge?)")
            } catch (e: Exception) {
                coroutineContext.ensureActive()
                TrackLog.d(TAG, "  ✗ hifiresolver prepare failed: ${e.message}")
            }
            if (attempt + 1 < TOKEN_ATTEMPTS) delay(TOKEN_RETRY_DELAY_MS)
        }
        tokenBlockedUntilMs = System.currentTimeMillis() + TOKEN_BLOCK_MS
        return null
    }

    /** Visible for tests: whether [prepareToken] would currently short-circuit. */
    internal fun isTokenBlocked(): Boolean = System.currentTimeMillis() < tokenBlockedUntilMs

    /** Test hook: reset the challenge backoff between cases. */
    internal fun clearTokenBlock() {
        tokenBlockedUntilMs = 0L
    }

    private suspend fun queueJob(
        token: String,
        qobuzUrl: String,
        title: String,
        artist: String,
        formatId: Int,
    ): String? {
        val payload = "{\"url\":${q(qobuzUrl)},\"title\":${q(title)}," +
            "\"artist\":${q(artist)},\"formatId\":$formatId}"
        return try {
            val request = Request.Builder()
                .url("${base}/api/qobuz/queue")
                .post(payload.toRequestBody("application/json".toMediaType()))
                .header("Accept", "application/json, text/plain, */*")
                .header("X-DL-Token", token)
                .header("Referer", "${base}/")
                .header("Origin", base)
                .build()
            val body = client.newCall(request).execute().use { response ->
                if (!response.isSuccessful) return null
                response.body?.string()?.takeIf { it.isNotBlank() } ?: return null
            }
            json.decodeFromString<QueueResponse>(body).jobId?.takeIf { it.isNotBlank() }
        } catch (e: Exception) {
            coroutineContext.ensureActive()
            null
        }
    }

    private suspend fun pollJob(jobId: String, formatId: Int): ResolvedFile? {
        repeat(MAX_POLLS_PER_FORMAT) {
            coroutineContext.ensureActive()
            delay(POLL_DELAY_MS)
            val job = try {
                val body = get("${base}/api/qobuz/jobs/$jobId")
                json.decodeFromString<JobResponse>(body)
            } catch (e: Exception) {
                coroutineContext.ensureActive()
                return null
            }
            when (job.status) {
                "done" -> {
                    val file = job.file?.takeIf { it.isNotBlank() } ?: return null
                    val name = encode(job.prettyName?.takeIf { it.isNotBlank() } ?: file)
                    return ResolvedFile(
                        url = "${base}/qobuz/files/$file?name=$name",
                        formatId = formatId,
                    )
                }
                "error" -> return null
                else -> Unit // queued/working: next round
            }
        }
        return null
    }

    /**
     * Best-first conversion across [formatIds] inside one wall-clock budget.
     * Null covers everything: no token, queue refused, jobs erroring, budget
     * spent. Every one of those is a miss for the caller to walk past, never
     * a fault.
     */
    suspend fun resolveStream(
        qobuzUrl: String,
        title: String,
        artist: String,
        formatIds: List<Int>,
        budgetMs: Long = RESOLVE_BUDGET_MS,
    ): ResolvedFile? {
        if (qobuzUrl.isBlank() || formatIds.isEmpty()) return null
        val token = prepareToken() ?: return null
        val deadline = System.currentTimeMillis() + budgetMs
        for (formatId in formatIds) {
            coroutineContext.ensureActive()
            if (System.currentTimeMillis() >= deadline) break
            val jobId = queueJob(token, qobuzUrl, title, artist, formatId) ?: continue
            val file = pollJob(jobId, formatId)
            if (file != null) return file
        }
        return null
    }

    // ── Matching ──────────────────────────────────────────────────────
    // Ported from the plugin's title logic: strip featured/remaster/version
    // noise and refuse a result carrying a modifier the request lacks, so a
    // live cut never stands in for the studio recording.

    private val MODIFIERS = listOf(
        "karaoke", "remix", "live", "instrumental", "version", "edit",
        "acoustic", "reprise", "demo", "session", "cover", "tribute",
        "orchestral", "piano",
    )

    fun normalizeTitle(title: String): String {
        var t = title.replace(Regex("[áàäâã]"), "a")
            .replace(Regex("[éèëê]"), "e")
            .replace(Regex("[íìïî]"), "i")
            .replace(Regex("[óòöôõ]"), "o")
            .replace(Regex("[úùüû]"), "u")
            .replace("ñ", "n").replace("Ñ", "N")
            .trim()
        t = t.substringBefore("(").trim()
        t = t.substringBefore("[").trim()
        t = t.substringBefore(" - ").trim()
        val lower = t.lowercase()
        t = t.substringBefore(" feat", t).trim().let {
            if (lower.contains(" feat")) it.substring(0, lower.indexOf(" feat")).trim() else it
        }
        val lower2 = t.lowercase()
        t = if (" ft." in lower2) t.substring(0, lower2.indexOf(" ft.")).trim() else t
        val lower3 = t.lowercase()
        t = if ("remaster" in lower3) t.substring(0, lower3.indexOf("remaster")).trim() else t
        return t
    }

    fun titlesMatch(search: String, result: String): Boolean {
        val s = normalizeTitle(search).lowercase()
        val r = normalizeTitle(result).lowercase()
        if (s.isEmpty() || r.isEmpty()) return false
        return r.contains(s) || s.contains(r)
    }

    fun hasModifier(title: String): Boolean {
        val lower = title.lowercase()
        return MODIFIERS.any { it in lower }
    }

    /** A result is playable for a request when titles match and it adds no new modifier. */
    fun isPlayableMatch(request: String, result: String): Boolean {
        if (!titlesMatch(request, result)) return false
        return !(hasModifier(result) && !hasModifier(request))
    }

    // ── Transport ─────────────────────────────────────────────────────

    private suspend fun get(url: String): String {
        val request = Request.Builder()
            .url(url)
            .header("Accept", "application/json")
            .header("Referer", "${base}/")
            .header("Origin", base)
            .build()
        return client.newCall(request).execute().use { response ->
            if (!response.isSuccessful) throw IllegalStateException("HTTP ${response.code}")
            response.body?.string()?.takeIf { it.isNotBlank() }
                ?: throw IllegalStateException("Empty response")
        }
    }

    private fun encode(value: String): String = java.net.URLEncoder.encode(value, "UTF-8")

    private fun q(value: String): String = "\"" + value
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", " ") + "\""
}
