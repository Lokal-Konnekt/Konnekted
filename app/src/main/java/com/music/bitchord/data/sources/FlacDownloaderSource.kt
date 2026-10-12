package com.music.bitchord.data.sources

import com.music.bitchord.data.TrackLog
import com.music.bitchord.data.model.Song
import com.music.bitchord.data.settings.AppSettings
import java.util.Locale
import kotlin.coroutines.coroutineContext
import kotlinx.coroutines.ensureActive

/**
 * Community Hi-Res FLAC through two backends under one source row.
 *
 * The primary backend is the downloader: Qobuz catalogue search, Deezer ISRC
 * precision matching, and queued conversions up to 24-bit/192kHz. No
 * account, no key — the backend holds whatever it holds.
 *
 * The fallback backend is direct FLAC. When conversions are gated or the
 * downloader has no row, the same source searches and streams verified FLAC
 * files directly, with quality inferred from measured size rather than
 * catalogue claims. It is deliberately not a separate source.
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
     * out in [search]. Parked on the API object — see
     * [FlacDownloaderApi.memoizeOrigin] for why an instance memo strands rows.
     */
    private fun memoize(packed: String, qobuzUrl: String, title: String, artist: String, durationSec: Int?) =
        FlacDownloaderApi.memoizeOrigin(
            packed,
            FlacDownloaderApi.Origin(qobuzUrl, title, artist, durationSec),
        )

    override suspend fun health(): SourceHealth {
        val catalogueOk = try {
            // An empty catalogue still proves the backend is there; only a
            // failed call means it isn't. (The search call itself never
            // throws — it answers empty — so the probe goes underneath it.)
            FlacDownloaderApi.probe()
            TrackLog.d(TAG, "  ✓ hifiresolver catalogue answers")
            true
        } catch (e: Exception) {
            coroutineContext.ensureActive()
            TrackLog.d(TAG, "  ✗ hifiresolver catalogue failed: ${e.message}")
            false
        }
        val directOk = HiResMonochromeBackend.probe()
        if (!catalogueOk) {
            if (directOk) return SourceHealth.Ok("direct FLAC ready · Qobuz catalogue down")
            return SourceHealth.Unreachable("no Hi-Res catalogue answered")
        }
        val directSuffix = if (directOk) " · direct FLAC ready" else ""
        // Second leg, stated plainly on the sources screen: a catalogue
        // that answers while conversions stay challenged streams nothing
        // from the downloader — but the direct backend still can.
        return if (FlacDownloaderApi.prepareToken() != null) {
            SourceHealth.Ok("flacdownloader.com · Qobuz FLAC$directSuffix")
        } else if (AppSettings.fdlSp.value.isBlank()) {
            SourceHealth.Ok("tap to unlock Qobuz$directSuffix")
        } else {
            SourceHealth.Ok("catalogue ok · conversions blocked on this network$directSuffix")
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
        val deezerRows = try {
            FlacDownloaderApi.deezerRows(q)
                .filter { FlacDownloaderApi.titlesMatch(q, it.title) }
        } catch (e: Exception) {
            coroutineContext.ensureActive()
            emptyList()
        }
        val isrcs = deezerRows.mapNotNull { it.isrc?.takeIf(String::isNotBlank) }
            .distinct()
            .take(3)
        val isrcHits = try {
            isrcs.flatMap { isrc -> FlacDownloaderApi.searchQobuz(isrc, 5) }
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
        val monoHits = try {
            HiResMonochromeBackend.rankByIsrc(
                HiResMonochromeBackend.searchTracks(q, limit.coerceIn(1, 50)),
                isrcs.firstOrNull(),
            ).filter { FlacDownloaderApi.isPlayableMatch(q, it.title) }
        } catch (e: Exception) {
            coroutineContext.ensureActive()
            TrackLog.d(TAG, "  ✗ hires direct catalogue search failed: ${e.message}")
            emptyList()
        }
        val flacRows = (isrcHits + textHits).distinctBy { it.id }
        val songs = flacRows.map { it.toSong() }.toMutableList()
        val seen = songs.mapTo(HashSet()) { catalogueKey(it.title, it.artist) }
        for (track in monoHits) {
            val title = track.title.ifBlank { "Unknown title" }
            val artist = track.credits
            if (!seen.add(catalogueKey(title, artist))) continue
            songs += track.toSong(title, artist)
        }
        val merged = songs.take(limit)
        if (merged.isNotEmpty()) TrackLog.d(TAG, "  ✓ hifiresolver ${merged.size} rows")
        return merged
    }

    private fun FlacDownloaderApi.QobuzTrack.toSong(): Song {
        val packed = id.toString()
        val qobuzUrl = url?.takeIf { it.isNotBlank() } ?: "https://open.qobuz.com/track/$id"
        val durationSec = durationMs?.takeIf { it > 0 }?.div(1000)?.toInt()
        memoize(packed, qobuzUrl, title, artist, durationSec)
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

    private fun catalogueKey(title: String, artist: String): String =
        "${FlacDownloaderApi.normalizeTitle(title).lowercase(Locale.ROOT)}|" +
            HiResMonochromeBackend.simplify(artist)

    private fun HiResMonochromeBackend.MonoTrack.toSong(title: String, artist: String): Song {
        val packed = HiResMonochromeBackend.packId(id)
        val durationSec = HiResMonochromeBackend.normalizeDurationMs(durationMs)
            ?.div(1000)?.toInt()
        HiResMonochromeBackend.memoizeOrigin(
            packed,
            HiResMonochromeBackend.Origin(id, HiResMonochromeBackend.normalizeDurationMs(durationMs)),
        )
        val duration = durationSec?.let { s -> "%d:%02d".format(Locale.ROOT, s / 60, s % 60) }
        return Song(
            videoId = SourceRegistry.trackKey(config.id, packed),
            title = title,
            artist = artist,
            albumName = album,
            thumbnailUrl = artwork?.takeIf { it.isNotBlank() },
            durationText = duration,
            sourceQuality = "LOSSLESS",
            isExplicit = explicit,
        )
    }

    override suspend fun stream(trackId: String, request: StreamRequest): SourceStream? {
        HiResMonochromeBackend.originOf(trackId)?.let { return streamDirect(it) }
        val origin = FlacDownloaderApi.originOf(trackId) ?: return null
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
        } ?: return monochromeFallback(origin.title, origin.artist, origin.durationSec)
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

    private suspend fun streamDirect(origin: HiResMonochromeBackend.Origin): SourceStream? {
        val file = try {
            HiResMonochromeBackend.resolveFile(origin.id, origin.durationMs)
        } catch (e: Exception) {
            coroutineContext.ensureActive()
            TrackLog.d(TAG, "  ✗ hires direct stream failed: ${e.message}")
            null
        } ?: return null
        TrackLog.d(TAG, "  ✓ hires direct ${file.kbps}kbps ${file.url.take(96)}")
        return SourceStream(
            url = file.url,
            format = com.music.bitchord.data.sources.StreamFormat(
                codec = "flac",
                kbps = file.kbps,
                sampleRateHz = file.sampleRateHz,
                bitDepth = file.bitDepth,
            ),
            durationSec = HiResMonochromeBackend.normalizeDurationMs(origin.durationMs)
                ?.div(1000)?.toInt(),
        )
    }

    /**
     * A downloader row that cannot convert falls back to the direct
     * catalogue before giving up. Title, artist credit and runtime all have
     * to agree, so a cover or a different cut cannot stand in for the row.
     */
    private suspend fun monochromeFallback(
        title: String,
        artist: String,
        durationSec: Int?,
    ): SourceStream? {
        val query = listOf(title, artist)
            .map { it.trim() }
            .filter { it.isNotEmpty() }
            .joinToString(" ")
            .takeIf { it.isNotEmpty() }
            ?: return null
        val candidates = try {
            HiResMonochromeBackend.searchTracks(query, 10)
        } catch (e: Exception) {
            coroutineContext.ensureActive()
            emptyList()
        }
        val hit = HiResMonochromeBackend.plausible(candidates, listOf(artist), null)
            .firstOrNull {
                FlacDownloaderApi.isPlayableMatch(title, it.title) &&
                    HiResMonochromeBackend.durationsMatch(durationSec, it.durationMs)
            } ?: return null
        TrackLog.d(TAG, "  ✓ hifiresolver fell back to direct catalogue")
        return streamDirect(
            HiResMonochromeBackend.Origin(
                hit.id,
                HiResMonochromeBackend.normalizeDurationMs(hit.durationMs),
            ),
        )
    }

    companion object {
        private const val TAG = "BitChord"
    }
}
