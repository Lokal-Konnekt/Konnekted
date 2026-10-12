package com.music.bitchord

import com.music.bitchord.data.sources.FlacDownloaderApi
import com.music.bitchord.data.sources.FlacDownloaderSource
import com.music.bitchord.data.sources.SourceConfig
import com.music.bitchord.data.sources.SourceKind
import com.music.bitchord.data.sources.SourceRegistry
import com.music.bitchord.data.sources.StreamRequest
import kotlinx.coroutines.runBlocking
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test

/**
 * The flacdownloader.com backend, end to end against a real HTTP server:
 * search mapping, the token → queue → poll handshake, tier fallback, and
 * every failure shaped as a miss rather than a fault.
 *
 * The live backend's `/prepare` is bot-gated from datacenter networks, so
 * no test here touches it — the mock stands in for a network the challenge
 * lets through, and what is proven is that this client drives the whole
 * exchange correctly once it does.
 */
class FlacDownloaderTest {

    private lateinit var server: MockWebServer
    private val routes = mutableMapOf<String, MockResponse>()
    private val seen = mutableListOf<RecordedRequest>()

    @Before
    fun setUp() {
        server = MockWebServer()
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                synchronized(seen) { seen += request }
                val path = request.requestUrl?.encodedPath.orEmpty()
                return routes[path] ?: MockResponse().setResponseCode(404)
            }
        }
        server.start()
        FlacDownloaderApi.baseOverride = server.url("/").toString().trimEnd('/')
    }

    @After
    fun tearDown() {
        FlacDownloaderApi.baseOverride = null
        server.shutdown()
    }

    private fun route(path: String, json: String, code: Int = 200) {
        routes[path] = MockResponse().setResponseCode(code).setBody(json)
    }

    private fun source() = FlacDownloaderSource(
        SourceConfig(kind = SourceKind.HIRES),
    )

    private fun searchJson(vararg titles: String) = """
        {"has_more":false,"total":${titles.size},"tracks":[
        ${titles.mapIndexed { i, t ->
        """{"id":${1000 + i},"title":"$t","artist":"The Artist","album":"The Album","cover":"https://art/$i.jpg","durationMs":200000,"isrc":"USXX10000$i","hires":true,"bitDepth":24,"samplingRate":44.1,"url":"https://open.qobuz.com/track/${1000 + i}","explicit":false}"""
    }.joinToString(",")}
        ]}
    """.trimIndent()

    // ── Search ──────────────────────────────────────────────────────

    @Test
    fun `search maps qobuz rows to tagged songs`() = runBlocking {
        route("/api/qobuz/search", searchJson("Blinding Lights (Remastered)", "Blinding Lights"))
        val src = source()
        val rows = src.search("Blinding Lights", limit = 10)
        assertEquals(2, rows.size)
        assertEquals("Blinding Lights (Remastered)", rows[0].title)
        assertEquals("The Artist", rows[0].artist)
        assertEquals("LOSSLESS", rows[0].sourceQuality)
        assertTrue(rows.all { it.videoId.startsWith("src:${src.configId}::") })
    }

    private fun innerId(videoId: String): String =
        SourceRegistry.parseTrackKey(videoId)?.second ?: error("not a track key: $videoId")

    @Test
    fun `search skips rows carrying modifiers the query lacks`() = runBlocking {
        route("/api/qobuz/search", searchJson("Blinding Lights Karaoke", "Blinding Lights"))
        val rows = source().search("Blinding Lights", limit = 10)
        assertEquals(1, rows.size)
        assertEquals("Blinding Lights", rows[0].title)
    }

    @Test
    fun `title matching ignores featured and remaster noise`() {
        assertTrue(FlacDownloaderApi.titlesMatch("Blinding Lights", "Blinding Lights (Remastered)"))
        assertTrue(FlacDownloaderApi.titlesMatch("Song feat. Someone", "Song"))
        assertTrue(!FlacDownloaderApi.titlesMatch("Blinding Lights", "Save Your Tears"))
    }

    // ── Full handshake ──────────────────────────────────────────────

    private fun prepareHandshake(
        token: String? = "tok123",
        jobId: String = "job9",
        file: String? = "track.flac",
        pretty: String = "Artist - Title.flac",
        pendingFirst: Int = 1,
    ) {
        route("/prepare", if (token == null) """{"error":"challenge_failed"}""" else """{"t":"$token"}""")
        route("/api/qobuz/queue", """{"id":"$jobId"}""")
        var polls = 0
        server.dispatcher = object : Dispatcher() {
            override fun dispatch(request: RecordedRequest): MockResponse {
                synchronized(seen) { seen += request }
                val path = request.requestUrl?.encodedPath.orEmpty()
                if (path == "/api/qobuz/jobs/$jobId" && file != null) {
                    polls++
                    if (polls <= pendingFirst) return MockResponse().setResponseCode(200).setBody("""{"status":"working"}""")
                    return MockResponse().setResponseCode(200)
                        .setBody("""{"status":"done","file":"$file","pretty_name":"$pretty"}""")
                }
                return routes[path] ?: MockResponse().setResponseCode(404)
            }
        }
    }

    @Test
    fun `stream walks prepare queue poll to a flac file`() = runBlocking {
        route("/api/qobuz/search", searchJson("Blinding Lights"))
        prepareHandshake()
        val src = source()
        val trackId = innerId(src.search("Blinding Lights", limit = 5).first().videoId)
        val stream = src.stream(trackId, StreamRequest.Lossless)
        assertNotNull(stream)
        assertTrue(stream!!.url.contains("/qobuz/files/track.flac"))
        assertEquals("flac", stream.format.codec)
        assertEquals(24, stream.format.bitDepth)
        assertEquals(192_000, stream.format.sampleRateHz)
        assertEquals(1411, stream.format.kbps)
        assertEquals(false, stream.belowRequest)
        assertEquals(200, stream.durationSec)
    }

    @Test
    fun `challenge on prepare is a miss, not a fault`() = runBlocking {
        route("/api/qobuz/search", searchJson("Blinding Lights"))
        prepareHandshake(token = null)
        val src = source()
        val trackId = innerId(src.search("Blinding Lights", limit = 5).first().videoId)
        assertNull(src.stream(trackId, StreamRequest.Lossless))
    }

    @Test
    fun `mp3 fallback is marked below a lossless request`() = runBlocking {
        // Search tested separately; drive resolveStream through the mock
        // directly for the tier assertion.
        prepareHandshake()
        val file = FlacDownloaderApi.resolveStream(
            qobuzUrl = "https://open.qobuz.com/track/1",
            title = "T",
            artist = "A",
            formatIds = listOf(FlacDownloaderApi.F_MP3_320),
        )
        assertNotNull(file)
        assertEquals(false, file!!.isLossless)
    }

    @Test
    fun `health is ok when the catalogue answers`() = runBlocking {
        route("/api/qobuz/search", searchJson("x"))
        val health = source().health()
        assertTrue(health.isOk)
    }

    @Test
    fun `health is unreachable when the catalogue is down`() = runBlocking {
        route("/api/qobuz/search", "error", 500)
        val health = source().health()
        assertTrue(!health.isOk)
    }

    @Test
    fun `kind carries the branded display name`() {
        assertEquals("BitChord Hi-Res Lossless Addon", SourceRegistry.HIRES_DISPLAY_NAME)
        assertEquals(SourceKind.HIRES, source().kind)
    }
}
