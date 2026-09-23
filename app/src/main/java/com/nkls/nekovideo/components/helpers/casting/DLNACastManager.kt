package com.nkls.nekovideo.components.helpers

import android.content.Context
import android.content.Intent
import android.media.MediaExtractor
import android.media.MediaFormat
import android.media.MediaMetadataRetriever
import android.net.wifi.WifiManager
import android.util.Log
import android.widget.Toast
import com.nkls.nekovideo.DebugTraceLogger
import fi.iki.elonen.NanoHTTPD
import kotlinx.coroutines.*
import java.io.File
import java.net.*
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * DLNA/UPnP cast manager — open-source replacement for Google Cast SDK.
 *
 * Discovery: SSDP multicast (UDP 239.255.255.250:1900)
 * File serving: LocalVideoServer (NanoHTTPD, port 8080)
 * Playback control: UPnP AvTransport via SOAP/HTTP
 */
class DLNACastManager(private val context: Context) {

    companion object {
        @Volatile private var instance: DLNACastManager? = null

        fun getInstance(context: Context): DLNACastManager =
            instance ?: synchronized(this) {
                instance ?: DLNACastManager(context.applicationContext).also { instance = it }
            }
    }

    private val tag = "DLNACastManager"

    data class DLNADevice(
        val name: String,
        val controlUrl: String,
        val baseUrl: String
    )

    private data class SoapResult(
        val httpStatus: Int? = null,
        val body: String? = null,
        val errorCode: String? = null,
        val errorDescription: String? = null,
        val exceptionMessage: String? = null
    ) {
        val isSuccess: Boolean
            get() = httpStatus != null && httpStatus in 200..299 && errorCode == null
    }

    private var videoServer: LocalVideoServer? = null
    private var connectedDevice: DLNADevice? = null
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    // Playlist state
    private var playlist = listOf<String>()
    private var playlistTitles = listOf<String>()
    private var currentIndex = 0

    // Playback state (updated via polling)
    var isConnected = false
        private set
    val connectedDeviceName: String get() = connectedDevice?.name ?: ""
    var isPlaying = false
        private set
    var currentPositionMs = 0L
        private set
    var durationMs = 0L
        private set
    var currentTitle = ""
        private set
    var currentVideoPath = ""
        private set

    var onConnectionStateChanged: ((Boolean) -> Unit)? = null
    var onDevicesFound: ((List<DLNADevice>) -> Unit)? = null
    var onStateChanged: (() -> Unit)? = null
    // Separate observer for DLNACastService (avoids overwriting onStateChanged from UI)
    var onServiceStateChanged: (() -> Unit)? = null

    private var stoppedByUser = false
    private var isLoadingTrack = false

    private var connectionListener: ((Boolean) -> Unit)? = null

    fun setConnectionStatusListener(listener: (Boolean) -> Unit) {
        connectionListener = listener
    }

    // ── Discovery ────────────────────────────────────────────────────────────

    fun discoverDevices() {
        scope.launch {
            val found = mutableListOf<DLNADevice>()

            val wifiManager = context.applicationContext
                .getSystemService(Context.WIFI_SERVICE) as WifiManager
            val multicastLock = wifiManager.createMulticastLock("nekovideo_ssdp").apply {
                setReferenceCounted(false)
                acquire()
            }
            Log.d(tag, "MulticastLock acquired: ${multicastLock.isHeld}")

            try {
                // Resolve WiFi interface via WifiManager IP (reliable on API 30+)
                @Suppress("DEPRECATION")
                val wifiIpInt = wifiManager.connectionInfo.ipAddress
                val wifiAddr = if (wifiIpInt != 0) {
                    InetAddress.getByAddress(
                        ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(wifiIpInt).array()
                    )
                } else null
                Log.d(tag, "WiFi IP from WifiManager: $wifiAddr")

                val wifiIface = wifiAddr?.let { addr ->
                    NetworkInterface.getNetworkInterfaces()
                        ?.asSequence()
                        ?.firstOrNull { iface ->
                            iface.inetAddresses.asSequence().any { it == addr }
                        }
                } ?: NetworkInterface.getNetworkInterfaces()
                    ?.asSequence()
                    ?.firstOrNull { iface ->
                        iface.isUp && !iface.isLoopback &&
                            iface.inetAddresses.asSequence().any { it is Inet4Address && !it.isLoopbackAddress }
                    }
                Log.d(tag, "Using network interface: ${wifiIface?.name} / ${wifiIface?.inetAddresses?.asSequence()?.toList()}")

                val group = InetAddress.getByName("239.255.255.250")
                val bindAddr = wifiAddr ?: InetAddress.getByName("0.0.0.0")

                val socket = MulticastSocket(null).apply {
                    // Bind to WiFi IP so send and receive both use WiFi interface
                    bind(InetSocketAddress(bindAddr, 0))
                    soTimeout = 500
                    if (wifiIface != null) {
                        networkInterface = wifiIface          // forces multicast SEND on WiFi
                        joinGroup(InetSocketAddress(group, 1900), wifiIface)
                    } else {
                        @Suppress("DEPRECATION")
                        joinGroup(group)
                    }
                }
                Log.d(tag, "Socket bound to ${socket.localAddress}:${socket.localPort}")

                val search = buildString {
                    append("M-SEARCH * HTTP/1.1\r\n")
                    append("HOST: 239.255.255.250:1900\r\n")
                    append("MAN: \"ssdp:discover\"\r\n")
                    append("MX: 3\r\n")
                    append("ST: urn:schemas-upnp-org:device:MediaRenderer:1\r\n\r\n")
                }
                val buf = search.toByteArray()

                // Send M-SEARCH twice — some devices miss the first packet
                repeat(2) {
                    socket.send(DatagramPacket(buf, buf.size, group, 1900))
                    Log.d(tag, "M-SEARCH sent (attempt ${it + 1})")
                    delay(200)
                }

                val deadline = System.currentTimeMillis() + 4000L
                val respBuf = ByteArray(4096)
                var packetCount = 0
                while (System.currentTimeMillis() < deadline) {
                    try {
                        val pkt = DatagramPacket(respBuf, respBuf.size)
                        socket.receive(pkt)
                        packetCount++
                        val response = String(pkt.data, 0, pkt.length)
                        Log.d(tag, "SSDP packet #$packetCount from ${pkt.address}:\n$response")

                        val location = extractHeader(response, "LOCATION")
                        if (location == null) {
                            Log.d(tag, "  → no LOCATION header, skipping")
                            continue
                        }
                        Log.d(tag, "  → fetching device description: $location")
                        val device = fetchDeviceDescription(location)
                        if (device != null && found.none { it.baseUrl == device.baseUrl }) {
                            Log.d(tag, "  → device added: ${device.name} @ ${device.controlUrl}")
                            found.add(device)
                        } else if (device == null) {
                            Log.w(tag, "  → fetchDeviceDescription returned null for $location")
                        }
                    } catch (_: SocketTimeoutException) {
                        // keep looping until deadline
                    }
                }
                Log.d(tag, "Discovery done. Packets received: $packetCount, devices found: ${found.size}")
                socket.close()
            } catch (e: Exception) {
                Log.e(tag, "SSDP discovery error", e)
            } finally {
                multicastLock.release()
                Log.d(tag, "MulticastLock released")
            }

            withContext(Dispatchers.Main) {
                onDevicesFound?.invoke(found)
            }
        }
    }

    private fun fetchDeviceDescription(location: String): DLNADevice? {
        return try {
            val url = URL(location)
            val conn = (url.openConnection() as HttpURLConnection).apply {
                connectTimeout = 2000
                readTimeout = 2000
            }
            val xml = conn.inputStream.bufferedReader().readText()
            conn.disconnect()

            val friendlyName = extractXmlTag(xml, "friendlyName") ?: "DLNA Device"
            val baseUrl = "${url.protocol}://${url.host}:${url.port}"

            for (block in xml.split("<service>")) {
                if (block.contains("AVTransport", ignoreCase = true)) {
                    val path = extractXmlTag(block, "controlURL") ?: continue
                    val controlUrl = if (path.startsWith("http")) path else "$baseUrl$path"
                    return DLNADevice(friendlyName, controlUrl, baseUrl)
                }
            }
            null
        } catch (e: Exception) {
            Log.w(tag, "fetchDeviceDescription failed: $location — ${e.message}")
            null
        }
    }

    // ── Connection ───────────────────────────────────────────────────────────

    fun connectToDevice(device: DLNADevice) {
        connectedDevice = device
        isConnected = true
        connectionListener?.invoke(true)
        onConnectionStateChanged?.invoke(true)
        context.startService(Intent(context, com.nkls.nekovideo.DLNACastService::class.java))
        startPolling()
    }

    private fun startPolling() {
        scope.launch {
            var wasPlaying = false
            var lastTransportState: String? = null
            while (isConnected) {
                try {
                    val pos = getPositionInfo()
                    if (pos != null) {
                        currentPositionMs = pos.first
                        durationMs = pos.second
                    }

                    val transportState = getTransportStateOrNull()
                    if (transportState != null) {
                        isPlaying = transportState == "PLAYING"
                        if (transportState != lastTransportState) {
                            trace("TV transport state ${lastTransportState ?: "<initial>"} -> $transportState")
                            lastTransportState = transportState
                        }

                        // Auto-advance when video ends naturally (PLAYING → STOPPED/NO_MEDIA_PRESENT)
                        // Guard isLoadingTrack: Smart TVs briefly enter STOPPED during SetAVTransportURI
                        // which would otherwise trigger a spurious next() and skip the intended video.
                        if (wasPlaying && !isPlaying && !stoppedByUser && !isLoadingTrack && playlist.size > 1
                            && transportState != "PAUSED_PLAYBACK") {
                            withContext(Dispatchers.Main) { next() }
                        }

                        wasPlaying = isPlaying
                    }
                    withContext(Dispatchers.Main) {
                        onStateChanged?.invoke()
                        onServiceStateChanged?.invoke()
                    }
                } catch (_: Exception) {
                }
                delay(500)
            }
        }
    }

    // ── Casting ──────────────────────────────────────────────────────────────

    fun castVideo(videoPath: String, videoTitle: String) {
        trace("CAST 1 castVideo entered title=$videoTitle")
        playlist = listOf(videoPath)
        playlistTitles = listOf(videoTitle)
        currentIndex = 0
        if (!prepareServer()) { trace("CAST prepareServer failed"); return }
        trace("CAST 2 server ready")
        loadAndPlay(videoPath, videoTitle)
    }

    fun castPlaylist(videosPaths: List<String>, videosTitles: List<String>, startIndex: Int = 0) {
        playlist = videosPaths
        playlistTitles = videosTitles
        currentIndex = startIndex
        if (!prepareServer()) return
        val path = videosPaths.getOrElse(startIndex) { return }
        val title = videosTitles.getOrElse(startIndex) { File(path.removePrefix("file://")).nameWithoutExtension }
        loadAndPlay(path, title)
    }

    private fun prepareServer(): Boolean {
        val server = videoServer ?: try {
            LocalVideoServer(context, 8080).also {
                it.start(NanoHTTPD.SOCKET_READ_TIMEOUT, false)
                videoServer = it
                Log.d(tag, "Local video server started on port 8080")
            }
        } catch (e: Exception) {
            Log.e(tag, "Failed to start local video server on port 8080", e)
            videoServer = null
            return false
        }

        server.clearVideos()

        playlist.forEach { path ->
            if (path.startsWith("locked://")) {
                val filePath = path.removePrefix("locked://")
                val xorKey = LockedPlaybackSession.getXorKeyForFile(filePath)
                val obfuscatedName = File(filePath).name
                val originalName = LockedPlaybackSession.getOriginalName(obfuscatedName) ?: obfuscatedName
                if (xorKey != null) {
                    server.addLockedVideo(filePath, xorKey, originalName)
                } else {
                    server.addVideo(filePath)
                }
            } else {
                server.addVideo(path.removePrefix("file://"))
            }
        }
        return true
    }

    private fun videoUrlFor(videoPath: String): String {
        val name = if (videoPath.startsWith("locked://")) {
            val obfuscated = File(videoPath.removePrefix("locked://")).name
            LockedPlaybackSession.getOriginalName(obfuscated) ?: obfuscated
        } else {
            File(videoPath.removePrefix("file://")).name
        }
        val ip = videoServer?.getLocalIpAddress() ?: "127.0.0.1"
        val encoded = URLEncoder.encode(name, "UTF-8").replace("+", "%20")
        return "http://$ip:8080/video/$encoded"
    }

    private fun mimeTypeFor(videoPath: String): String {
        val name = if (videoPath.startsWith("locked://")) {
            val obfuscated = File(videoPath.removePrefix("locked://")).name
            LockedPlaybackSession.getOriginalName(obfuscated) ?: obfuscated
        } else {
            File(videoPath.removePrefix("file://")).name
        }
        return mimeTypeForVideoFileName(name)
    }

    private data class DlnaMediaInfo(
        val size: Long? = null,
        val durationMs: Long? = null,
        val width: Int? = null,
        val height: Int? = null,
        val videoMime: String? = null,
        val audioMime: String? = null,
        val videoProfile: Int? = null,
        val videoLevel: Int? = null,
        val audioProfile: Int? = null,
        val audioChannelCount: Int? = null,
        val audioSampleRate: Int? = null,
        val audioBitrate: Int? = null
    )

    /**
     * Read only container/track headers. The media is never transcoded or copied.
     * If probing fails we keep the old, minimal DLNA metadata as a safe fallback.
     */
    private fun probeMedia(videoPath: String): DlnaMediaInfo {
        if (videoPath.startsWith("locked://")) return DlnaMediaInfo()
        val file = File(videoPath.removePrefix("file://"))
        if (!file.isFile) return DlnaMediaInfo()

        var durationMs: Long? = null
        var width: Int? = null
        var height: Int? = null
        try {
            MediaMetadataRetriever().use { retriever ->
                retriever.setDataSource(file.absolutePath)
                durationMs = retriever.extractMetadata(MediaMetadataRetriever.METADATA_KEY_DURATION)?.toLongOrNull()
                width = retriever.extractMetadata(MediaMetadataRetriever.METADATA_KEY_VIDEO_WIDTH)?.toIntOrNull()
                height = retriever.extractMetadata(MediaMetadataRetriever.METADATA_KEY_VIDEO_HEIGHT)?.toIntOrNull()
            }
        } catch (e: Exception) {
            Log.w(tag, "Metadata retriever failed for ${file.name}: ${e.message}")
        }

        var videoMime: String? = null
        var audioMime: String? = null
        var videoProfile: Int? = null
        var videoLevel: Int? = null
        var audioProfile: Int? = null
        var audioChannelCount: Int? = null
        var audioSampleRate: Int? = null
        var audioBitrate: Int? = null
        val extractor = MediaExtractor()
        try {
            extractor.setDataSource(file.absolutePath)
            trace("MEDIA probe file=${file.name} trackCount=${extractor.trackCount}")
            for (i in 0 until extractor.trackCount) {
                val format = extractor.getTrackFormat(i)
                val mime = format.getString(MediaFormat.KEY_MIME)

                fun intValue(key: String): Int? = runCatching {
                    if (format.containsKey(key)) format.getInteger(key) else null
                }.getOrNull()

                val profile = intValue(MediaFormat.KEY_PROFILE)
                    ?: intValue(MediaFormat.KEY_AAC_PROFILE)
                val level = intValue(MediaFormat.KEY_LEVEL)
                val channels = intValue(MediaFormat.KEY_CHANNEL_COUNT)
                val sampleRate = intValue(MediaFormat.KEY_SAMPLE_RATE)
                val bitrate = intValue(MediaFormat.KEY_BIT_RATE)
                val csd = (0..2).mapNotNull { index ->
                    runCatching { format.getByteBuffer("csd-$index")?.remaining() }
                        .getOrNull()
                        ?.let { size -> "csd-$index=$size" }
                }.joinToString(",")

                trace(
                    buildString {
                        append("MEDIA track[$i] mime=${mime ?: "<none>"}")
                        if (profile != null) append(" profile=$profile")
                        if (level != null) append(" level=$level")
                        if (channels != null) append(" channels=$channels")
                        if (sampleRate != null) append(" sampleRate=$sampleRate")
                        if (bitrate != null) append(" bitrate=$bitrate")
                        if (csd.isNotEmpty()) append(" $csd")
                    }
                )

                if (mime?.startsWith("video/") == true && videoMime == null) {
                    videoMime = mime
                    videoProfile = profile
                    videoLevel = level
                }
                if (mime?.startsWith("audio/") == true && audioMime == null) {
                    audioMime = mime
                    audioProfile = profile
                    audioChannelCount = channels
                    audioSampleRate = sampleRate
                    audioBitrate = bitrate
                }
            }
        } catch (e: Exception) {
            Log.w(tag, "MediaExtractor failed for ${file.name}: ${e.message}")
            trace("MEDIA probe failed file=${file.name} error=${e.javaClass.simpleName}: ${e.message}")
        } finally {
            extractor.release()
        }

        if (audioMime == null) {
            trace("MEDIA warning file=${file.name} audio track not identified")
        }

        return DlnaMediaInfo(
            size = file.length(),
            durationMs = durationMs,
            width = width,
            height = height,
            videoMime = videoMime,
            audioMime = audioMime,
            videoProfile = videoProfile,
            videoLevel = videoLevel,
            audioProfile = audioProfile,
            audioChannelCount = audioChannelCount,
            audioSampleRate = audioSampleRate,
            audioBitrate = audioBitrate
        )
    }

    private fun formatDlnaDuration(durationMs: Long): String {
        val totalSeconds = durationMs / 1000
        val millis = durationMs % 1000
        return "%d:%02d:%02d.%03d".format(
            totalSeconds / 3600,
            (totalSeconds % 3600) / 60,
            totalSeconds % 60,
            millis
        )
    }

    private fun dlnaProfileName(mimeType: String, info: DlnaMediaInfo): String? {
        // Start with profiles that can be identified from Android's parsed track metadata
        // without guessing. Unknown combinations deliberately omit DLNA.ORG_PN.
        if (mimeType != "video/mp4" || info.videoMime != "video/avc" || info.audioMime != "audio/mp4a-latm") return null

        val width = info.width ?: return null
        val height = info.height ?: return null
        val profile = info.videoProfile ?: return null
        val level = info.videoLevel ?: return null
        val isSd = width <= 720 && height <= 576

        // MediaCodecInfo.CodecProfileLevel values: AVCProfileBaseline=0x01,
        // AVCLevel3=0x100, AVCLevel31=0x200. Bubble/Samsung capture showed
        // AVC_MP4_BL_L3L_SD_AAC for the matching baseline/SD family.
        return if (isSd && profile == 0x01 && level <= 0x200) {
            "AVC_MP4_BL_L3L_SD_AAC"
        } else null
    }

    private fun buildDIDLMetadata(
        title: String,
        url: String,
        mimeType: String,
        info: DlnaMediaInfo
    ): String {
        // OP=01 advertises byte-range support. 017... mirrors the richer Samsung-compatible
        // form observed from BubbleUPnP while keeping profile-name advertising conservative:
        // a wrong DLNA.ORG_PN is worse than omitting it.
        val profileName = dlnaProfileName(mimeType, info)
        val additionalInfo = buildString {
            if (profileName != null) append("DLNA.ORG_PN=$profileName;")
            append("DLNA.ORG_OP=01;DLNA.ORG_FLAGS=01700000000000000000000000000000")
        }
        val protocolInfo = "http-get:*:$mimeType:$additionalInfo"
        val attributes = buildString {
            info.size?.takeIf { it > 0 }?.let { append(" size=\"$it\"") }
            info.durationMs?.takeIf { it > 0 }?.let { append(" duration=\"${formatDlnaDuration(it)}\"") }
            if (info.width != null && info.height != null && info.width!! > 0 && info.height!! > 0) {
                append(" resolution=\"${info.width}x${info.height}\"")
            }
        }
        Log.d(tag, "DLNA media: mime=$mimeType video=${info.videoMime} profile=${info.videoProfile} level=${info.videoLevel} audio=${info.audioMime} audioProfile=${info.audioProfile} channels=${info.audioChannelCount} sampleRate=${info.audioSampleRate} bitrate=${info.audioBitrate} size=${info.size} duration=${info.durationMs} resolution=${info.width}x${info.height} dlnaProfile=$profileName")
        val didl = """<DIDL-Lite xmlns="urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:upnp="urn:schemas-upnp-org:metadata-1-0/upnp/"><item id="0" parentID="-1" restricted="false"><dc:title>${title.escapeXml()}</dc:title><upnp:class>object.item.videoItem</upnp:class><res protocolInfo="$protocolInfo"$attributes>${url.escapeXml()}</res></item></DIDL-Lite>"""
        return didl.escapeXml()
    }

    private fun loadAndPlay(videoPath: String, videoTitle: String) {
        trace("CAST 3 loadAndPlay entered title=$videoTitle")
        val device = connectedDevice ?: return
        currentTitle = videoTitle
        currentVideoPath = videoPath
        stoppedByUser = false
        isPlaying = false
        isLoadingTrack = true
        scope.launch {
            try {
                val url = videoUrlFor(videoPath)
                trace("CAST 4 URL ready url=$url")
                val mime = mimeTypeFor(videoPath)
                trace("CAST 5 probeMedia START mime=$mime")
                val mediaInfo = probeMedia(videoPath)
                trace(
                    "CAST 6 probeMedia END video=${mediaInfo.videoMime} audio=${mediaInfo.audioMime} " +
                        "audioProfile=${mediaInfo.audioProfile} channels=${mediaInfo.audioChannelCount} " +
                        "sampleRate=${mediaInfo.audioSampleRate} bitrate=${mediaInfo.audioBitrate}"
                )
                val metadata = buildDIDLMetadata(videoTitle, url, mime, mediaInfo)
                trace("CAST 7 DIDL ready")

                stopTransportForMediaSwitch(device.controlUrl, "before-set-uri")

                trace("CAST 8 SetAVTransportURI START")
                var setUriResult = sendSoapCommand(
                    device.controlUrl,
                    "SetAVTransportURI",
                    "<CurrentURI>${url.escapeXml()}</CurrentURI><CurrentURIMetaData>$metadata</CurrentURIMetaData>"
                )

                if (!setUriResult.isSuccess && setUriResult.errorCode == "705") {
                    trace("CAST SetAVTransportURI returned 705; stopping transport and retrying once")
                    stopTransportForMediaSwitch(device.controlUrl, "705-retry")
                    setUriResult = sendSoapCommand(
                        device.controlUrl,
                        "SetAVTransportURI",
                        "<CurrentURI>${url.escapeXml()}</CurrentURI><CurrentURIMetaData>$metadata</CurrentURIMetaData>"
                    )
                }

                trace("CAST 9 SetAVTransportURI END success=${setUriResult.isSuccess}")
                if (!setUriResult.isSuccess) {
                    showCastError("SetAVTransportURI", setUriResult)
                    return@launch
                }

                var state = getTransportStateOrNull()
                if (state == "TRANSITIONING") {
                    state = waitForTransportState(
                        expected = setOf("STOPPED", "PLAYING", "PAUSED_PLAYBACK", "NO_MEDIA_PRESENT"),
                        timeoutMs = 15_000,
                        reason = "after-set-uri"
                    )
                }
                trace("CAST post-URI state=${state ?: "unknown"}")

                if (state == "PLAYING") {
                    trace("CAST Play skipped: renderer already PLAYING")
                    return@launch
                }

                trace("CAST 10 Play START")
                var playResult = sendSoapCommand(device.controlUrl, "Play", "<Speed>1</Speed>")

                if (!playResult.isSuccess && playResult.errorCode == "701") {
                    val current = getTransportStateOrNull()
                    if (current == "TRANSITIONING") {
                        val settled = waitForTransportState(
                            expected = setOf("STOPPED", "PLAYING", "PAUSED_PLAYBACK", "NO_MEDIA_PRESENT"),
                            timeoutMs = 15_000,
                            reason = "play-701"
                        )
                        if (settled == "PLAYING") {
                            trace("CAST Play 701 ignored: renderer reached PLAYING on its own")
                            return@launch
                        }
                        if (settled == "STOPPED" || settled == "PAUSED_PLAYBACK") {
                            trace("CAST Play retry after 701 state=$settled")
                            playResult = sendSoapCommand(device.controlUrl, "Play", "<Speed>1</Speed>")
                        }
                    }
                }

                trace("CAST 11 Play END success=${playResult.isSuccess}")
                if (!playResult.isSuccess) {
                    showCastError("Play", playResult)
                    return@launch
                }

                val playingState = waitForTransportState(
                    expected = setOf("PLAYING"),
                    timeoutMs = 15_000,
                    reason = "after-play"
                )
                if (playingState != "PLAYING") {
                    trace("CAST warning: Play returned success but renderer did not reach PLAYING")
                }
            } catch (e: Exception) {
                Log.e(tag, "loadAndPlay error", e)
                trace("CAST ERROR ${e.javaClass.simpleName}: ${e.message}")
                showCastError("Cast", SoapResult(exceptionMessage = e.message))
            } finally {
                isLoadingTrack = false
            }
        }
    }

    private suspend fun stopTransportForMediaSwitch(controlUrl: String, reason: String): Boolean {
        var state = getTransportStateOrNull()
        trace("CAST stop-check[$reason] state=${state ?: "unknown"}")

        if (state == null || state == "STOPPED" || state == "NO_MEDIA_PRESENT") return true

        var stopResult = sendSoapCommand(controlUrl, "Stop", "")
        if (!stopResult.isSuccess && stopResult.errorCode == "701" && state == "TRANSITIONING") {
            state = waitForTransportState(
                expected = setOf("STOPPED", "PLAYING", "PAUSED_PLAYBACK", "NO_MEDIA_PRESENT"),
                timeoutMs = 10_000,
                reason = "$reason-stop-701"
            )
            if (state == "STOPPED" || state == "NO_MEDIA_PRESENT") return true
            trace("CAST Stop retry after 701 state=${state ?: "unknown"}")
            stopResult = sendSoapCommand(controlUrl, "Stop", "")
        }

        if (!stopResult.isSuccess) {
            trace(
                "CAST Stop failed[$reason] HTTP=${stopResult.httpStatus} " +
                    "UPnP=${stopResult.errorCode} description=${stopResult.errorDescription}"
            )
            return false
        }

        val stoppedState = waitForTransportState(
            expected = setOf("STOPPED", "NO_MEDIA_PRESENT"),
            timeoutMs = 10_000,
            reason = "$reason-stopped"
        )
        val stopped = stoppedState == "STOPPED" || stoppedState == "NO_MEDIA_PRESENT"
        if (!stopped) {
            trace("CAST Stop timeout[$reason] last=${stoppedState ?: "unknown"}")
        }
        return stopped
    }

    private suspend fun waitForTransportState(
        expected: Set<String>,
        timeoutMs: Long,
        reason: String
    ): String? {
        val deadline = System.currentTimeMillis() + timeoutMs
        var lastState: String? = null
        while (isConnected && System.currentTimeMillis() < deadline) {
            val state = getTransportStateOrNull()
            if (state != null && state != lastState) {
                trace("CAST wait[$reason] state=$state")
                lastState = state
            }
            if (state != null && state in expected) return state
            delay(250)
        }
        trace("CAST wait[$reason] timeout last=${lastState ?: "unknown"}")
        return lastState
    }

    // ── Playback controls ────────────────────────────────────────────────────

    fun play() {
        val device = connectedDevice ?: return
        scope.launch {
            val result = sendSoapCommand(device.controlUrl, "Play", "<Speed>1</Speed>")
            if (!result.isSuccess) showCastError("Play", result)
        }
    }

    fun pause() {
        val device = connectedDevice ?: return
        scope.launch {
            val result = sendSoapCommand(device.controlUrl, "Pause", "")
            if (!result.isSuccess) showCastError("Pause", result)
        }
    }

    fun seekTo(posMs: Long) {
        val device = connectedDevice ?: return
        scope.launch {
            val state = getTransportStateOrNull()
            if (state != "PLAYING" && state != "PAUSED_PLAYBACK") {
                trace("CAST Seek skipped state=${state ?: "unknown"} target=${msToTimeString(posMs)}")
                return@launch
            }
            val result = sendSoapCommand(
                device.controlUrl,
                "Seek",
                "<Unit>REL_TIME</Unit><Target>${msToTimeString(posMs)}</Target>"
            )
            if (!result.isSuccess) showCastError("Seek", result)
        }
    }
    fun next() {
        if (playlist.isEmpty()) return
        currentIndex = (currentIndex + 1) % playlist.size
        val path = playlist[currentIndex]
        val title = playlistTitles.getOrElse(currentIndex) { File(path.removePrefix("file://")).nameWithoutExtension }
        loadAndPlay(path, title)
    }

    fun previous() {
        if (playlist.isEmpty()) return
        currentIndex = (currentIndex - 1 + playlist.size) % playlist.size
        val path = playlist[currentIndex]
        val title = playlistTitles.getOrElse(currentIndex) { File(path.removePrefix("file://")).nameWithoutExtension }
        loadAndPlay(path, title)
    }

    fun stopPlayback() {
        stoppedByUser = true
        playlist = listOf()
        playlistTitles = listOf()
        currentIndex = 0
        currentTitle = ""
        currentVideoPath = ""
        isPlaying = false
        currentPositionMs = 0L
        durationMs = 0L
        connectedDevice?.let { scope.launch { sendSoap(it.controlUrl, "Stop", "") } }
        scope.launch(Dispatchers.Main) { onStateChanged?.invoke() }
    }

    fun stopCasting() {
        stoppedByUser = true
        connectedDevice?.let { scope.launch { sendSoap(it.controlUrl, "Stop", "") } }
        disconnect()
    }

    fun disconnect() {
        isConnected = false
        isPlaying = false
        connectedDevice = null
        connectionListener?.invoke(false)
        onConnectionStateChanged?.invoke(false)
        context.stopService(Intent(context, com.nkls.nekovideo.DLNACastService::class.java))
        stopServer()
    }

    fun destroy() {
        scope.cancel()
        stopServer()
    }

    private fun stopServer() {
        videoServer?.stop()
        videoServer = null
    }

    // ── UPnP state queries ───────────────────────────────────────────────────

    private fun getPositionInfo(): Pair<Long, Long>? {
        val device = connectedDevice ?: return null
        return try {
            val response = sendSoap(device.controlUrl, "GetPositionInfo", "") ?: return null
            val pos = parseTimeString(extractXmlTag(response, "RelTime") ?: "0:00:00")
            val dur = parseTimeString(extractXmlTag(response, "TrackDuration") ?: "0:00:00")
            Pair(pos, dur)
        } catch (_: Exception) { null }
    }

    private fun getTransportStateOrNull(): String? {
        val device = connectedDevice ?: return null
        return try {
            val response = sendSoap(device.controlUrl, "GetTransportInfo", "") ?: return null
            extractXmlTag(response, "CurrentTransportState")
        } catch (_: Exception) {
            null
        }
    }

    private fun getTransportState(): String =
        getTransportStateOrNull() ?: "UNKNOWN"

    // ── SOAP ─────────────────────────────────────────────────────────────────

    private fun sendSoap(controlUrl: String, action: String, args: String): String? {
        val result = sendSoapRequest(controlUrl, action, args, persistentTrace = false)
        return if (result.isSuccess) result.body else null
    }

    private fun sendSoapCommand(controlUrl: String, action: String, args: String): SoapResult =
        sendSoapRequest(controlUrl, action, args, persistentTrace = true)

    private fun sendSoapRequest(
        controlUrl: String,
        action: String,
        args: String,
        persistentTrace: Boolean
    ): SoapResult {
        var conn: HttpURLConnection? = null
        return try {
            val soap = """<?xml version="1.0"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
            s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
  <s:Body>
    <u:$action xmlns:u="urn:schemas-upnp-org:service:AVTransport:1">
      <InstanceID>0</InstanceID>
      $args
    </u:$action>
  </s:Body>
</s:Envelope>"""
            val url = URL(controlUrl)
            conn = (url.openConnection() as HttpURLConnection).apply {
                requestMethod = "POST"
                doOutput = true
                connectTimeout = 3000
                readTimeout = 3000
                setRequestProperty("Content-Type", "text/xml; charset=\"utf-8\"")
                setRequestProperty(
                    "SOAPAction",
                    "\"urn:schemas-upnp-org:service:AVTransport:1#$action\""
                )
            }
            val startedAt = System.currentTimeMillis()
            Log.d(tag, "SOAP -> $action url=$controlUrl")
            if (persistentTrace) trace("SOAP -> $action")
            conn.outputStream.use { it.write(soap.toByteArray(Charsets.UTF_8)) }
            val status = conn.responseCode
            val response = try {
                conn.inputStream?.bufferedReader()?.use { it.readText() }
            } catch (_: Exception) {
                conn.errorStream?.bufferedReader()?.use { it.readText() }
            }
            val errorCode = response?.let { extractXmlTag(it, "errorCode") }
            val errorDescription = response?.let { extractXmlTag(it, "errorDescription") }
            val elapsed = System.currentTimeMillis() - startedAt
            Log.d(tag, "SOAP <- $action HTTP $status in ${elapsed}ms body=${response?.take(1000)}")
            if (persistentTrace) {
                val fault = buildString {
                    if (!errorCode.isNullOrBlank()) append(" UPnP=$errorCode")
                    if (!errorDescription.isNullOrBlank()) append(" description=$errorDescription")
                }
                trace("SOAP <- $action HTTP $status in ${elapsed}ms$fault")
            }
            SoapResult(
                httpStatus = status,
                body = response,
                errorCode = errorCode,
                errorDescription = errorDescription
            )
        } catch (e: Exception) {
            Log.w(tag, "SOAP $action failed: ${e.message}")
            if (persistentTrace) trace("SOAP <- $action EXCEPTION ${e.javaClass.simpleName}: ${e.message}")
            SoapResult(exceptionMessage = e.message)
        } finally {
            conn?.disconnect()
        }
    }

    private fun showCastError(action: String, result: SoapResult) {
        val description = result.errorDescription?.takeIf { it.isNotBlank() } ?: when (result.errorCode) {
            "701" -> "Transition not available"
            "705" -> "Transport is locked"
            "710" -> "Seek mode not supported"
            "711" -> "Illegal seek target"
            "716" -> "Resource not found"
            else -> null
        }
        val detail = when {
            !result.errorCode.isNullOrBlank() -> buildString {
                append("UPnP ${result.errorCode}")
                if (!description.isNullOrBlank()) append(": $description")
            }
            result.httpStatus != null -> "HTTP ${result.httpStatus}"
            !result.exceptionMessage.isNullOrBlank() -> result.exceptionMessage
            else -> "Unknown error"
        }
        val label = when (action) {
            "SetAVTransportURI" -> "Load media"
            "Play" -> "Play"
            "Pause" -> "Pause"
            "Seek" -> "Seek"
            else -> action
        }
        trace("CAST FAILURE action=$action detail=$detail")
        scope.launch(Dispatchers.Main) {
            Toast.makeText(context, "Cast failed: $label ($detail)", Toast.LENGTH_LONG).show()
        }
    }
    // ── Helpers ──────────────────────────────────────────────────────────────

    private fun extractHeader(response: String, header: String): String? =
        response.lines()
            .firstOrNull { it.startsWith("$header:", ignoreCase = true) }
            ?.substringAfter(":")?.trim()

    private fun extractXmlTag(xml: String, tag: String): String? {
        val start = xml.indexOf("<$tag>").takeIf { it >= 0 } ?: return null
        val end = xml.indexOf("</$tag>", start).takeIf { it >= 0 } ?: return null
        return xml.substring(start + tag.length + 2, end).trim()
    }

    private fun msToTimeString(ms: Long): String {
        val s = ms / 1000
        return "%d:%02d:%02d".format(s / 3600, (s % 3600) / 60, s % 60)
    }

    private fun parseTimeString(time: String): Long {
        return try {
            val parts = time.split(":").map { it.toLong() }
            when (parts.size) {
                3 -> (parts[0] * 3600 + parts[1] * 60 + parts[2]) * 1000
                2 -> (parts[0] * 60 + parts[1]) * 1000
                else -> 0L
            }
        } catch (_: Exception) { 0L }
    }

    private fun trace(message: String) {
        Log.d(tag, message)
        DebugTraceLogger.log(context, message)
    }

    private fun String.escapeXml() = replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
}
