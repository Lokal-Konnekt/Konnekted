package com.music.bitchord.data.sources

import com.music.bitchord.data.TrackLog
import com.music.bitchord.data.model.Song
import java.util.Collections
import java.util.Locale
import kotlin.coroutines.coroutineContext
import kotlinx.coroutines.ensureActive

/**
 * Community Hi-Res FLAC through a downloader backend: Qobuz catalogue
 * search, Deezer ISRC precision matching, and queued conversions up to
 * 24-bit/192kHz. No account, no key — the backend holds whatever it holds.
 *
 * The protocol is the one the Spotube FLAC plugin speaks, reimplemented
 * against plain HTTP: that plugin needs a Hetu runtime and Spotube's host
 * APIs, neither of which exists here, but its half-dozen calls reduce to
 * JSON this app can ask for directly. Conversion is a *job*, not a URL, so
 * resolution is budgeted and cancellable: a slow backend loses the
 * resolver's race instead of holding it.
 */
class FlacDownloaderSource(
    override val config: SourceConfig,
) : MusicSource, SourceRegistry.ConfigBacked {

    override val configId: String get() = config.id
    override val kind: SourceKind get() = SourceKind.HIRES

    override val displayName: String get() = config.label.ifBlank {
        SourceRegistry.HIRES_DISPLAY_NAME
    }

    /**
     * What a search row needs later at queue time, by the packed id handed
     * out in [search]. The queue call wants the Qobuz URL plus the title
     * and artist it was matched under — none of which belong in a track id
     * that travels through queues, history and notifications.
     */
    private data class Origin(
        val qobuzUrl: String,
        val title: String,
        val artist: String,
        val durationSec: Int?,
    )

    private val origins = Collections.synchronizedMap(
        object : LinkedHashMap<String, Origin>(64, 0.75f, true) {
            override fun removeEldestEntry(eldest: Map.Entry<String, Origin>) = size > MAX_ORIGINS
        },
    )

    override suspend fun health(): SourceHealth {
        return try {
            // An empty catalogue still proves the backend is there; only a
            // failed call means it isn't. (The search call itself never
            // throws — it answers empty — so the probe goes underneath it.)
            FlacDownloaderApi.probe()
            TrackLog.d(TAG, "  ✓ hifiresolver catalogue answers")
            SourceHealth.Ok("flacdownloader.com · Qobuz FLAC")
        } catch (e: Exception) {
            coroutineContext.ensureActive()
            SourceHealth.Unreachable(e.message ?: "No answer")
        }
    }

    override suspend fun search(
        query: String,
        limit: Int,
        waitForAll: Boolean,
        request: StreamRequest?,
    ): List<Song> {
        val q = query.trim().takeIf { it.isNotEmpty() } ?: return emptyList()
        // ISRC precision first, exactly like the plugin: Deezer names the
        // recording, Qobuz is asked for that ISRC, and text search is the
        // fallback rather than the authority.
        val isrcHits = try {
            FlacDownloaderApi.deezerRows(q)
                .filter { FlacDownloaderApi.titlesMatch(q, it.title) }
                .mapNotNull { it.isrc?.takeIf(String::isNotBlank) }
                .distinct()
                .take(3)
                .flatMap { isrc -> FlacDownloaderApi.searchQobuz(isrc, 5) }
                .filter { FlacDownloaderApi.isPlayableMatch(q, it.title) }
        } catch (e: Exception) {
            coroutineContext.ensureActive()
            emptyList()
        }
        val textHits = try {
            FlacDownloaderApi.searchQobuz(q, limit.coerceIn(1, 50))
                .filter { FlacDownloaderApi.isPlayableMatch(q, it.title) }
        } catch (e: Exception) {
            coroutineContext.ensureActive()
            TrackLog.d(TAG, "  ✗ hifiresolver search failed: ${e.message}")
            emptyList()
        }
        val rows = (isrcHits + textHits).distinctBy { it.id }.take(limit)
        if (rows.isNotEmpty()) TrackLog.d(TAG, "  ✓ hifiresolver ${rows.size} rows")
        return rows.map { it.toSong() }
    }

    private fun FlacDownloaderApi.QobuzTrack.toSong(): Song {
        val packed = id.toString()
        val qobuzUrl = url?.takeIf { it.isNotBlank() } ?: "https://open.qobuz.com/track/$id"
        val durationSec = durationMs?.takeIf { it > 0 }?.div(1000)?.toInt()
        origins[packed] = Origin(qobuzUrl, title, artist, durationSec)
        val duration = durationSec?.let { s -> "%d:%02d".format(Locale.ROOT, s / 60, s % 60) }
        return Song(
            videoId = SourceRegistry.trackKey(config.id, packed),
            title = title.ifBlank { "Unknown title" },
            artist = artist.ifBlank { "Unknown artist" },
            albumName = album.takeIf { it.isNotBlank() },
            thumbnailUrl = cover?.takeIf { it.isNotBlank() },
            durationText = duration,
            sourceQuality = if (hires || (bitDepth ?: 0.0) >= 24.0) "LOSSLESS" else null,
            isExplicit = explicit,
        )
    }

    override suspend fun stream(trackId: String, request: StreamRequest): SourceStream? {
        val origin = origins[trackId] ?: return null
        val formatIds = when (request) {
            // Best-first; a lower FLAC that answers is still lossless.
            is StreamRequest.Lossless -> FlacDownloaderApi.LOSSLESS_FORMATS
            is StreamRequest.Capped -> if (request.maxKbps >= 1000) {
                FlacDownloaderApi.LOSSLESS_FORMATS + FlacDownloaderApi.LOSSY_FORMATS
            } else {
                FlacDownloaderApi.LOSSY_FORMATS
            }
            // The backend's best lossy rendition.
            is StreamRequest.Best -> FlacDownloaderApi.LOSSY_FORMATS
        }
        val file = try {
            FlacDownloaderApi.resolveStream(origin.qobuzUrl, origin.title, origin.artist, formatIds)
        } catch (e: Exception) {
            coroutineContext.ensureActive()
            TrackLog.d(TAG, "  ✗ hifiresolver stream failed: ${e.message}")
            null
        } ?: return null
        TrackLog.d(TAG, "  ✓ hifiresolver ${file.formatId} ${file.url.take(96)}")
        val mp3 = file.formatId == FlacDownloaderApi.F_MP3_320
        return SourceStream(
            url = file.url,
            format = com.music.bitchord.data.sources.StreamFormat(
                codec = if (mp3) "mp3" else "flac",
                kbps = file.kbps,
                sampleRateHz = file.sampleRateHz,
                bitDepth = file.bitDepth,
            ),
            belowRequest = mp3 && request is StreamRequest.Lossless,
            durationSec = origin.durationSec,
        )
    }

    companion object {
        private const val TAG = "BitChord"
        private const val MAX_ORIGINS = 128
    }
}
