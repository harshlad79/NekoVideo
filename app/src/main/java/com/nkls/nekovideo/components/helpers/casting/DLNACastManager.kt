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
import kotlinx.coroutines.channels.Channel
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

    enum class CastControlState {
        DISCONNECTED,
        IDLE,
        PREPARING,
        READY_PLAYING,
        READY_PAUSED,
        BROWSING,
        ERROR
    }

    private data class CastRequest(
        val generation: Long,
        val videoPath: String,
        val videoTitle: String,
        val startPositionMs: Long,
        val seekOnStart: Boolean
    )

    private data class RendererSnapshot(
        val state: String,
        val positionMs: Long,
        val durationMs: Long
    )

    private var videoServer: LocalVideoServer? = null
    private var connectedDevice: DLNADevice? = null
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val castRequests = Channel<CastRequest>(Channel.CONFLATED)
    private val activeSeekRequests = Channel<Unit>(Channel.CONFLATED)
    @Volatile private var latestRequestGeneration = 0L
    @Volatile private var desiredPositionMs = 0L
    @Volatile private var desiredPositionExplicit = false
    @Volatile private var latestActiveSeekVersion = 0L
    @Volatile private var activeSeekInProgress = false
    private var confirmedVideoPath = ""
    private var confirmedVideoUrl = ""

    private val castWorkerJob = scope.launch {
        for (request in castRequests) {
            processCastRequest(request)
        }
    }

    private val activeSeekWorkerJob = scope.launch {
        for (ignored in activeSeekRequests) {
            if (!activeSeekInProgress) continue
            val device = connectedDevice ?: continue
            val expectedUrl = confirmedVideoUrl
            if (expectedUrl.isEmpty()) continue
            processLatestActiveSeek(
                device = device,
                expectedUrl = expectedUrl,
                sessionGeneration = latestRequestGeneration
            )
        }
    }

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
    var controlState = CastControlState.DISCONNECTED
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
        controlState = CastControlState.IDLE
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
                    val canQueryRenderer = !activeSeekInProgress &&
                        (controlState == CastControlState.READY_PLAYING ||
                            controlState == CastControlState.READY_PAUSED)

                    if (canQueryRenderer) {
                        val pos = getPositionInfo()
                        val canApplyPosition = !activeSeekInProgress &&
                            (controlState == CastControlState.READY_PLAYING ||
                                controlState == CastControlState.READY_PAUSED)
                        if (pos != null && canApplyPosition) {
                            currentPositionMs = pos.first
                            durationMs = pos.second
                        }
                    }

                    val transportState = getTransportStateOrNull()
                    if (transportState != null) {
                        val rendererIsPlaying = transportState == "PLAYING"
                        if (transportState != lastTransportState) {
                            trace("TV transport state ${lastTransportState ?: "<initial>"} -> $transportState")
                            lastTransportState = transportState
                        }

                        val canApplyTransport = !activeSeekInProgress &&
                            (controlState == CastControlState.READY_PLAYING ||
                                controlState == CastControlState.READY_PAUSED)
                        if (canApplyTransport) {
                            isPlaying = rendererIsPlaying
                            when (transportState) {
                                "PLAYING" -> controlState = CastControlState.READY_PLAYING
                                "PAUSED_PLAYBACK" -> controlState = CastControlState.READY_PAUSED
                            }
                        }

                        if (wasPlaying && !rendererIsPlaying && !stoppedByUser && !isLoadingTrack &&
                            playlist.size > 1 && transportState != "PAUSED_PLAYBACK") {
                            withContext(Dispatchers.Main) { next() }
                        }

                        wasPlaying = rendererIsPlaying
                    }
                    notifyStateChanged()
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
        if (videoServer != null) return true
        return try {
            LocalVideoServer(context, 8080).also {
                it.start(NanoHTTPD.SOCKET_READ_TIMEOUT, false)
                videoServer = it
                Log.d(tag, "Local video server started on port 8080")
            }
            true
        } catch (e: Exception) {
            Log.e(tag, "Failed to start local video server on port 8080", e)
            videoServer = null
            false
        }
    }

    private fun registerVideoWithServer(videoPath: String): Boolean {
        val server = videoServer ?: return false
        if (videoPath.startsWith("locked://")) {
            val filePath = videoPath.removePrefix("locked://")
            val xorKey = LockedPlaybackSession.getXorKeyForFile(filePath)
            val obfuscatedName = File(filePath).name
            val originalName = LockedPlaybackSession.getOriginalName(obfuscatedName) ?: obfuscatedName
            if (xorKey != null) {
                server.addLockedVideo(filePath, xorKey, originalName)
            } else {
                server.addVideo(filePath)
            }
        } else {
            server.addVideo(videoPath.removePrefix("file://"))
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

    @Synchronized
    private fun nextRequestGeneration(): Long {
        latestRequestGeneration += 1
        return latestRequestGeneration
    }

    @Synchronized
    private fun nextActiveSeekVersion(): Long {
        latestActiveSeekVersion += 1
        return latestActiveSeekVersion
    }

    private fun isCurrentRequest(request: CastRequest): Boolean =
        request.generation == latestRequestGeneration

    private fun notifyStateChangedAsync() {
        scope.launch { notifyStateChanged() }
    }

    private suspend fun notifyStateChanged() {
        withContext(Dispatchers.Main) {
            onStateChanged?.invoke()
            onServiceStateChanged?.invoke()
        }
    }

    private fun loadAndPlay(
        videoPath: String,
        videoTitle: String,
        startPositionMs: Long = 0L,
        seekOnStart: Boolean = false
    ) {
        val generation = nextRequestGeneration()
        val initialPosition = startPositionMs.coerceAtLeast(0L)
        desiredPositionMs = initialPosition
        desiredPositionExplicit = seekOnStart
        activeSeekInProgress = false
        nextActiveSeekVersion()

        currentTitle = videoTitle
        currentVideoPath = videoPath
        currentPositionMs = initialPosition
        durationMs = 0L
        stoppedByUser = false
        isPlaying = false
        isLoadingTrack = true
        controlState = CastControlState.PREPARING

        trace("CAST queue generation=$generation title=$videoTitle")
        notifyStateChangedAsync()

        val result = castRequests.trySend(
            CastRequest(
                generation = generation,
                videoPath = videoPath,
                videoTitle = videoTitle,
                startPositionMs = initialPosition,
                seekOnStart = seekOnStart
            )
        )
        if (result.isFailure) {
            trace("CAST queue failed generation=$generation")
            if (generation == latestRequestGeneration) {
                isLoadingTrack = false
                controlState = CastControlState.ERROR
                notifyStateChangedAsync()
            }
        }
    }

    private suspend fun processCastRequest(request: CastRequest) {
        if (!isCurrentRequest(request)) {
            trace("CAST drop superseded generation=${request.generation}")
            return
        }

        trace("CAST 3 loadAndPlay entered generation=${request.generation} title=${request.videoTitle}")
        val device = connectedDevice ?: return

        try {
            val url = videoUrlFor(request.videoPath)
            trace("CAST 4 URL ready generation=${request.generation} url=$url")
            val mime = mimeTypeFor(request.videoPath)
            trace("CAST 5 probeMedia START generation=${request.generation} mime=$mime")
            val mediaInfo = probeMedia(request.videoPath)
            trace(
                "CAST 6 probeMedia END generation=${request.generation} video=${mediaInfo.videoMime} " +
                    "audio=${mediaInfo.audioMime} audioProfile=${mediaInfo.audioProfile} " +
                    "channels=${mediaInfo.audioChannelCount} sampleRate=${mediaInfo.audioSampleRate} " +
                    "bitrate=${mediaInfo.audioBitrate}"
            )

            if (!isCurrentRequest(request)) {
                trace("CAST superseded after probe generation=${request.generation}")
                return
            }

            durationMs = mediaInfo.durationMs ?: 0L
            val upperBound = durationMs.takeIf { it > 0L } ?: Long.MAX_VALUE
            currentPositionMs = desiredPositionMs.coerceIn(0L, upperBound)
            notifyStateChanged()

            val metadata = buildDIDLMetadata(request.videoTitle, url, mime, mediaInfo)
            trace("CAST 7 DIDL ready generation=${request.generation}")

            stopTransportForMediaSwitch(
                controlUrl = device.controlUrl,
                reason = "before-set-uri",
                generation = request.generation
            )
            if (!isCurrentRequest(request)) {
                trace("CAST superseded after stop generation=${request.generation}")
                return
            }

            if (!registerVideoWithServer(request.videoPath)) {
                trace("CAST server registration failed generation=${request.generation}")
                markRequestError(request)
                return
            }

            trace("CAST 8 SetAVTransportURI START generation=${request.generation}")
            var setUriResult = sendSoapCommand(
                device.controlUrl,
                "SetAVTransportURI",
                "<CurrentURI>${url.escapeXml()}</CurrentURI><CurrentURIMetaData>$metadata</CurrentURIMetaData>"
            )

            if (!setUriResult.isSuccess && setUriResult.errorCode == "705" && isCurrentRequest(request)) {
                trace("CAST SetAVTransportURI returned 705; stopping transport and retrying once")
                stopTransportForMediaSwitch(
                    controlUrl = device.controlUrl,
                    reason = "705-retry",
                    generation = request.generation
                )
                if (!isCurrentRequest(request)) return
                setUriResult = sendSoapCommand(
                    device.controlUrl,
                    "SetAVTransportURI",
                    "<CurrentURI>${url.escapeXml()}</CurrentURI><CurrentURIMetaData>$metadata</CurrentURIMetaData>"
                )
            }

            trace("CAST 9 SetAVTransportURI END generation=${request.generation} success=${setUriResult.isSuccess}")
            if (!setUriResult.isSuccess) {
                if (isCurrentRequest(request)) {
                    showCastError("SetAVTransportURI", setUriResult)
                    markRequestError(request)
                }
                return
            }
            if (!isCurrentRequest(request)) {
                trace("CAST superseded after SetAVTransportURI generation=${request.generation}")
                return
            }

            var state = getTransportStateOrNull()
            if (state == "TRANSITIONING") {
                state = waitForTransportState(
                    expected = setOf("STOPPED", "PLAYING", "PAUSED_PLAYBACK", "NO_MEDIA_PRESENT"),
                    timeoutMs = 15_000,
                    reason = "after-set-uri",
                    generation = request.generation
                )
            }
            if (!isCurrentRequest(request)) return
            trace("CAST post-URI generation=${request.generation} state=${state ?: "unknown"}")

            if (state != "PLAYING") {
                trace("CAST 10 Play START generation=${request.generation}")
                var playResult = sendSoapCommand(device.controlUrl, "Play", "<Speed>1</Speed>")

                if (!playResult.isSuccess && playResult.errorCode == "701" && isCurrentRequest(request)) {
                    val current = getTransportStateOrNull()
                    if (current == "TRANSITIONING") {
                        val settled = waitForTransportState(
                            expected = setOf("STOPPED", "PLAYING", "PAUSED_PLAYBACK", "NO_MEDIA_PRESENT"),
                            timeoutMs = 15_000,
                            reason = "play-701",
                            generation = request.generation
                        )
                        if (settled == "PLAYING") {
                            trace("CAST Play 701 ignored: renderer reached PLAYING on its own")
                            playResult = SoapResult(httpStatus = 200)
                        } else if (settled == "STOPPED" || settled == "PAUSED_PLAYBACK") {
                            trace("CAST Play retry after 701 state=$settled")
                            playResult = sendSoapCommand(device.controlUrl, "Play", "<Speed>1</Speed>")
                        }
                    }
                }

                trace("CAST 11 Play END generation=${request.generation} success=${playResult.isSuccess}")
                if (!playResult.isSuccess) {
                    if (isCurrentRequest(request)) {
                        showCastError("Play", playResult)
                        markRequestError(request)
                    }
                    return
                }
            } else {
                trace("CAST Play skipped: renderer already PLAYING")
            }

            if (!isCurrentRequest(request)) return
            val shouldSeek = desiredPositionExplicit || request.seekOnStart
            val snapshot: RendererSnapshot

            if (shouldSeek) {
                val started = waitForPlaybackStartForSeek(
                    request = request,
                    expectedUrl = url,
                    timeoutMs = 15_000,
                    graceMs = 15_000
                )
                if (!started) {
                    if (isCurrentRequest(request)) {
                        trace("CAST warning: renderer did not reach PLAYING before pending seek")
                        markRequestError(request)
                    }
                    return
                }

                if (!isCurrentRequest(request)) return
                val targetPosition = desiredPositionMs
                trace("CAST apply pending seek generation=${request.generation} target=${msToTimeString(targetPosition)}")
                val seekResult = sendSoapCommand(
                    device.controlUrl,
                    "Seek",
                    "<Unit>REL_TIME</Unit><Target>${msToTimeString(targetPosition)}</Target>"
                )
                if (!seekResult.isSuccess) {
                    if (isCurrentRequest(request)) {
                        showCastError("Seek", seekResult)
                        markRequestError(request)
                    }
                    return
                }

                snapshot = waitForExpectedPlayback(
                    request = request,
                    expectedUrl = url,
                    timeoutMs = 15_000,
                    graceMs = 10_000,
                    reason = "after-start-seek",
                    expectedPositionMs = targetPosition
                ) ?: run {
                    if (isCurrentRequest(request)) markRequestError(request)
                    return
                }
            } else {
                snapshot = waitForExpectedPlayback(
                    request = request,
                    expectedUrl = url,
                    timeoutMs = 15_000,
                    graceMs = 15_000,
                    reason = "after-play"
                ) ?: run {
                    if (isCurrentRequest(request)) {
                        trace("CAST warning: renderer did not confirm expected media playback")
                        markRequestError(request)
                    }
                    return
                }
            }

            markRequestReady(request, url, snapshot)
        } catch (e: Exception) {
            Log.e(tag, "loadAndPlay error", e)
            trace("CAST ERROR generation=${request.generation} ${e.javaClass.simpleName}: ${e.message}")
            if (isCurrentRequest(request)) {
                showCastError("Cast", SoapResult(exceptionMessage = e.message))
                markRequestError(request)
            }
        }
    }

    private suspend fun waitForPlaybackStartForSeek(
        request: CastRequest,
        expectedUrl: String,
        timeoutMs: Long,
        graceMs: Long
    ): Boolean {
        var deadline = System.currentTimeMillis() + timeoutMs
        var graceUsed = false
        var lastState: String? = null
        var lastUri: String? = null

        while (isConnected && isCurrentRequest(request)) {
            val state = getTransportStateOrNull()
            if (state != null && state != lastState) {
                trace("CAST wait[pre-seek-play] generation=${request.generation} state=$state")
                lastState = state
            }

            if (state == "PLAYING") {
                val currentUri = getCurrentMediaUriOrNull()
                if (!currentUri.isNullOrBlank() && currentUri != lastUri) {
                    trace("CAST wait[pre-seek-play] generation=${request.generation} uri=$currentUri")
                    lastUri = currentUri
                }
                val uriMatches = currentUri.isNullOrBlank() || mediaUrisMatch(currentUri, expectedUrl)
                if (uriMatches) return true
            }

            if (System.currentTimeMillis() >= deadline) {
                val finalUri = getCurrentMediaUriOrNull()
                val finalState = getTransportStateOrNull()
                if (!finalUri.isNullOrBlank()) lastUri = finalUri
                if (finalState != null) lastState = finalState

                val uriMatches = finalUri.isNullOrBlank() || mediaUrisMatch(finalUri, expectedUrl)
                if (uriMatches && finalState == "PLAYING") {
                    trace("CAST wait[pre-seek-play] final recheck reached PLAYING generation=${request.generation}")
                    return true
                }

                if (!graceUsed && uriMatches && finalState == "TRANSITIONING") {
                    graceUsed = true
                    deadline = System.currentTimeMillis() + graceMs
                    trace("CAST wait[pre-seek-play] grace generation=${request.generation} +${graceMs}ms")
                } else {
                    trace(
                        "CAST wait[pre-seek-play] timeout/superseded generation=${request.generation} " +
                            "state=${lastState ?: "unknown"} uri=${lastUri ?: "unknown"}"
                    )
                    return false
                }
            }

            delay(100)
        }
        return false
    }

    private suspend fun waitForExpectedPlayback(
        request: CastRequest,
        expectedUrl: String,
        timeoutMs: Long,
        graceMs: Long = 0L,
        reason: String,
        expectedPositionMs: Long? = null
    ): RendererSnapshot? {
        var deadline = System.currentTimeMillis() + timeoutMs
        var graceUsed = false
        var lastState: String? = null
        var lastUri: String? = null
        var lastPositionMs: Long? = null

        while (isConnected && isCurrentRequest(request)) {
            val currentUri = getCurrentMediaUriOrNull()
            val state = getTransportStateOrNull()
            val pos = getPositionInfo()

            if (state != null && state != lastState) {
                trace("CAST wait[$reason] generation=${request.generation} state=$state")
                lastState = state
            }
            if (!currentUri.isNullOrBlank() && currentUri != lastUri) {
                trace("CAST wait[$reason] generation=${request.generation} uri=$currentUri")
                lastUri = currentUri
            }
            if (pos != null) lastPositionMs = pos.first

            val uriMatches = currentUri.isNullOrBlank() || mediaUrisMatch(currentUri, expectedUrl)
            val positionMatches = expectedPositionMs == null ||
                (pos != null && kotlin.math.abs(pos.first - expectedPositionMs) <= 3_000L)

            if (uriMatches && state == "PLAYING" && pos != null && pos.second > 0L && positionMatches) {
                return RendererSnapshot(state, pos.first, pos.second)
            }

            if (System.currentTimeMillis() >= deadline) {
                val finalUri = getCurrentMediaUriOrNull()
                val finalState = getTransportStateOrNull()
                val finalPos = getPositionInfo()
                if (!finalUri.isNullOrBlank()) lastUri = finalUri
                if (finalState != null) lastState = finalState
                if (finalPos != null) lastPositionMs = finalPos.first

                val finalUriMatches = finalUri.isNullOrBlank() || mediaUrisMatch(finalUri, expectedUrl)
                val finalPositionMatches = expectedPositionMs == null ||
                    (finalPos != null && kotlin.math.abs(finalPos.first - expectedPositionMs) <= 3_000L)

                if (finalUriMatches && finalState == "PLAYING" &&
                    finalPos != null && finalPos.second > 0L && finalPositionMatches) {
                    trace("CAST wait[$reason] final recheck succeeded generation=${request.generation}")
                    return RendererSnapshot(finalState, finalPos.first, finalPos.second)
                }

                if (!graceUsed && graceMs > 0L && finalUriMatches && finalState == "TRANSITIONING") {
                    graceUsed = true
                    deadline = System.currentTimeMillis() + graceMs
                    trace("CAST wait[$reason] grace generation=${request.generation} +${graceMs}ms")
                } else {
                    trace(
                        "CAST wait[$reason] timeout/superseded generation=${request.generation} " +
                            "state=${lastState ?: "unknown"} uri=${lastUri ?: "unknown"} " +
                            "position=${lastPositionMs?.let { msToTimeString(it) } ?: "unknown"} " +
                            "expected=${expectedPositionMs?.let { msToTimeString(it) } ?: "any"}"
                    )
                    return null
                }
            }

            delay(250)
        }
        return null
    }

    private suspend fun markRequestReady(
        request: CastRequest,
        expectedUrl: String,
        snapshot: RendererSnapshot
    ) {
        if (!isCurrentRequest(request)) return

        confirmedVideoPath = request.videoPath
        confirmedVideoUrl = expectedUrl
        currentPositionMs = snapshot.positionMs
        durationMs = snapshot.durationMs
        isPlaying = snapshot.state == "PLAYING"
        isLoadingTrack = false
        desiredPositionExplicit = false
        controlState = if (isPlaying) CastControlState.READY_PLAYING else CastControlState.READY_PAUSED
        trace("CAST READY generation=${request.generation} state=${snapshot.state} url=$expectedUrl")
        notifyStateChanged()
    }

    private suspend fun markRequestError(request: CastRequest) {
        if (!isCurrentRequest(request)) return
        isPlaying = false
        isLoadingTrack = false
        controlState = CastControlState.ERROR
        trace("CAST ERROR state generation=${request.generation}")
        notifyStateChanged()
    }

    private suspend fun stopTransportForMediaSwitch(
        controlUrl: String,
        reason: String,
        generation: Long? = null
    ): Boolean {
        if (generation != null && generation != latestRequestGeneration) return false
        var state = getTransportStateOrNull()
        trace("CAST stop-check[$reason] state=${state ?: "unknown"}")

        if (state == null || state == "STOPPED" || state == "NO_MEDIA_PRESENT") return true

        var stopResult = sendSoapCommand(controlUrl, "Stop", "")
        if (!stopResult.isSuccess && stopResult.errorCode == "701" && state == "TRANSITIONING") {
            state = waitForTransportState(
                expected = setOf("STOPPED", "PLAYING", "PAUSED_PLAYBACK", "NO_MEDIA_PRESENT"),
                timeoutMs = 10_000,
                reason = "$reason-stop-701",
                generation = generation
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
            reason = "$reason-stopped",
            generation = generation
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
        reason: String,
        generation: Long? = null
    ): String? {
        val deadline = System.currentTimeMillis() + timeoutMs
        var lastState: String? = null
        while (isConnected &&
            (generation == null || generation == latestRequestGeneration) &&
            System.currentTimeMillis() < deadline) {
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
        when (controlState) {
            CastControlState.BROWSING, CastControlState.ERROR -> {
                val path = currentVideoPath
                if (path.isNotEmpty()) {
                    loadAndPlay(
                        videoPath = path,
                        videoTitle = currentTitle,
                        startPositionMs = currentPositionMs,
                        seekOnStart = true
                    )
                }
            }
            CastControlState.READY_PAUSED -> {
                val device = connectedDevice ?: return
                isPlaying = false
                isLoadingTrack = true
                controlState = CastControlState.PREPARING
                notifyStateChangedAsync()
                scope.launch {
                    val result = sendSoapCommand(device.controlUrl, "Play", "<Speed>1</Speed>")
                    if (!result.isSuccess) {
                        showCastError("Play", result)
                        isLoadingTrack = false
                        controlState = CastControlState.ERROR
                        notifyStateChanged()
                        return@launch
                    }
                    confirmCurrentMediaState("resume-play")
                }
            }
            else -> Unit
        }
    }

    fun pause() {
        if (controlState != CastControlState.READY_PLAYING) return
        val device = connectedDevice ?: return
        isPlaying = false
        isLoadingTrack = true
        controlState = CastControlState.PREPARING
        notifyStateChangedAsync()
        scope.launch {
            val result = sendSoapCommand(device.controlUrl, "Pause", "")
            if (!result.isSuccess) {
                showCastError("Pause", result)
                isLoadingTrack = false
                controlState = CastControlState.ERROR
                notifyStateChanged()
                return@launch
            }
            confirmCurrentMediaState("pause")
        }
    }

    fun seekTo(posMs: Long) {
        val bounded = if (durationMs > 0L) {
            posMs.coerceIn(0L, durationMs)
        } else {
            posMs.coerceAtLeast(0L)
        }

        when (controlState) {
            CastControlState.PREPARING -> {
                desiredPositionMs = bounded
                desiredPositionExplicit = true
                currentPositionMs = bounded

                if (activeSeekInProgress) {
                    val version = nextActiveSeekVersion()
                    trace("CAST active seek updated version=$version target=${msToTimeString(bounded)}")
                    activeSeekRequests.trySend(Unit)
                } else {
                    trace("CAST pending seek generation=$latestRequestGeneration target=${msToTimeString(bounded)}")
                }
                notifyStateChangedAsync()
            }
            CastControlState.READY_PAUSED,
            CastControlState.BROWSING,
            CastControlState.ERROR -> beginBrowseAtPosition(bounded)
            CastControlState.READY_PLAYING -> {
                if (connectedDevice == null || confirmedVideoUrl.isEmpty()) return

                desiredPositionMs = bounded
                desiredPositionExplicit = true
                val seekVersion = nextActiveSeekVersion()
                activeSeekInProgress = true
                isPlaying = false
                isLoadingTrack = true
                controlState = CastControlState.PREPARING
                currentPositionMs = bounded
                trace("CAST active seek start version=$seekVersion target=${msToTimeString(bounded)}")
                notifyStateChangedAsync()
                activeSeekRequests.trySend(Unit)
            }
            else -> trace("CAST Seek skipped state=$controlState target=${msToTimeString(bounded)}")
        }
    }

    private suspend fun ensureRendererPlayingForActiveSeek(
        device: DLNADevice,
        sessionGeneration: Long
    ): Boolean {
        if (sessionGeneration != latestRequestGeneration) return false

        var state = getTransportStateOrNull()
        if (state == "TRANSITIONING") {
            state = waitForTransportState(
                expected = setOf("PLAYING", "PAUSED_PLAYBACK", "STOPPED", "NO_MEDIA_PRESENT"),
                timeoutMs = 5_000,
                reason = "active-seek-settle",
                generation = sessionGeneration
            )
        }

        if (sessionGeneration != latestRequestGeneration) return false
        if (state == null || state == "PLAYING") return true

        if (state == "PAUSED_PLAYBACK" || state == "STOPPED") {
            trace("CAST active seek restore Play from state=$state")
            val playResult = sendSoapCommand(device.controlUrl, "Play", "<Speed>1</Speed>")
            if (!playResult.isSuccess) {
                showCastError("Play", playResult)
                return false
            }
            val playing = waitForTransportState(
                expected = setOf("PLAYING"),
                timeoutMs = 5_000,
                reason = "active-seek-restore-play",
                generation = sessionGeneration
            )
            return playing == "PLAYING"
        }

        return false
    }

    private suspend fun processLatestActiveSeek(
        device: DLNADevice,
        expectedUrl: String,
        sessionGeneration: Long
    ) {
        try {
            while (isConnected &&
                activeSeekInProgress &&
                sessionGeneration == latestRequestGeneration &&
                confirmedVideoUrl == expectedUrl) {
                if (!ensureRendererPlayingForActiveSeek(device, sessionGeneration)) {
                    if (sessionGeneration == latestRequestGeneration && activeSeekInProgress) {
                        isPlaying = false
                        isLoadingTrack = false
                        controlState = CastControlState.ERROR
                        activeSeekInProgress = false
                        notifyStateChanged()
                    }
                    return
                }

                val seekVersion = latestActiveSeekVersion
                val target = desiredPositionMs
                trace("CAST active seek send version=$seekVersion target=${msToTimeString(target)}")

                val result = sendSoapCommand(
                    device.controlUrl,
                    "Seek",
                    "<Unit>REL_TIME</Unit><Target>${msToTimeString(target)}</Target>"
                )
                val seekAcceptedAtMs = System.currentTimeMillis()

                if (seekVersion != latestActiveSeekVersion) {
                    trace("CAST active seek superseded after command version=$seekVersion")
                    continue
                }

                if (!result.isSuccess) {
                    showCastError("Seek", result)
                    if (sessionGeneration == latestRequestGeneration) {
                        isPlaying = false
                        isLoadingTrack = false
                        controlState = CastControlState.ERROR
                        activeSeekInProgress = false
                        notifyStateChanged()
                    }
                    return
                }

                val snapshot = waitForActiveSeekConfirmation(
                    expectedUrl = expectedUrl,
                    expectedPositionMs = target,
                    seekAcceptedAtMs = seekAcceptedAtMs,
                    seekVersion = seekVersion,
                    sessionGeneration = sessionGeneration
                )

                if (seekVersion != latestActiveSeekVersion) {
                    trace("CAST active seek superseded while confirming version=$seekVersion")
                    continue
                }

                if (snapshot == null) {
                    if (sessionGeneration == latestRequestGeneration) {
                        isPlaying = false
                        isLoadingTrack = false
                        controlState = CastControlState.ERROR
                        activeSeekInProgress = false
                        notifyStateChanged()
                    }
                    return
                }

                currentPositionMs = snapshot.positionMs
                durationMs = snapshot.durationMs
                isPlaying = true
                isLoadingTrack = false
                desiredPositionExplicit = false
                activeSeekInProgress = false
                controlState = CastControlState.READY_PLAYING
                trace("CAST active seek READY version=$seekVersion position=${msToTimeString(snapshot.positionMs)}")
                notifyStateChanged()
                return
            }
        } finally {
            if (sessionGeneration != latestRequestGeneration || confirmedVideoUrl != expectedUrl) {
                activeSeekInProgress = false
            }
        }
    }

    private fun activeSeekPositionMatches(
        positionMs: Long,
        targetMs: Long,
        seekAcceptedAtMs: Long
    ): Boolean {
        val elapsedMs = (System.currentTimeMillis() - seekAcceptedAtMs).coerceAtLeast(0L)
        val minExpected = (targetMs - 3_000L).coerceAtLeast(0L)
        val maxExpected = targetMs + elapsedMs + 4_000L
        return positionMs in minExpected..maxExpected
    }

    private suspend fun waitForActiveSeekConfirmation(
        expectedUrl: String,
        expectedPositionMs: Long,
        seekAcceptedAtMs: Long,
        seekVersion: Long,
        sessionGeneration: Long
    ): RendererSnapshot? {
        var deadline = System.currentTimeMillis() + 10_000L
        var graceUsed = false
        var zeroSeekRecoveryAttempted = false
        var lastState: String? = null
        var lastPositionMs: Long? = null

        while (isConnected &&
            sessionGeneration == latestRequestGeneration &&
            seekVersion == latestActiveSeekVersion) {
            val uri = getCurrentMediaUriOrNull()
            val state = getTransportStateOrNull()
            val pos = getPositionInfo()

            if (state != null && state != lastState) {
                trace("CAST wait[active-seek] version=$seekVersion state=$state")
                lastState = state
            }
            if (pos != null) lastPositionMs = pos.first

            val uriMatches = uri.isNullOrBlank() || mediaUrisMatch(uri, expectedUrl)
            val positionMatches = pos != null &&
                activeSeekPositionMatches(pos.first, expectedPositionMs, seekAcceptedAtMs)

            if (uriMatches && expectedPositionMs <= 1_000L &&
                state == "STOPPED" && !zeroSeekRecoveryAttempted) {
                zeroSeekRecoveryAttempted = true
                val device = connectedDevice ?: return null
                trace("CAST active seek zero-position STOPPED; restoring Play version=$seekVersion")
                val playResult = sendSoapCommand(device.controlUrl, "Play", "<Speed>1</Speed>")
                if (!playResult.isSuccess) {
                    showCastError("Play", playResult)
                    return null
                }
                deadline = System.currentTimeMillis() + 10_000L
                delay(200)
                continue
            }

            if (uriMatches && state == "PLAYING" && pos != null &&
                pos.second > 0L && positionMatches) {
                return RendererSnapshot(state, pos.first, pos.second)
            }

            if (System.currentTimeMillis() >= deadline) {
                val finalUri = getCurrentMediaUriOrNull()
                val finalState = getTransportStateOrNull()
                val finalPos = getPositionInfo()
                val finalUriMatches = finalUri.isNullOrBlank() || mediaUrisMatch(finalUri, expectedUrl)
                val finalPositionMatches = finalPos != null &&
                    activeSeekPositionMatches(finalPos.first, expectedPositionMs, seekAcceptedAtMs)

                if (finalUriMatches && finalState == "PLAYING" &&
                    finalPos != null && finalPos.second > 0L && finalPositionMatches) {
                    return RendererSnapshot(finalState, finalPos.first, finalPos.second)
                }

                if (!graceUsed && finalUriMatches && finalState == "TRANSITIONING") {
                    graceUsed = true
                    deadline = System.currentTimeMillis() + 5_000L
                    trace("CAST wait[active-seek] grace version=$seekVersion +5000ms")
                } else {
                    val elapsedMs = (System.currentTimeMillis() - seekAcceptedAtMs).coerceAtLeast(0L)
                    trace(
                        "CAST wait[active-seek] timeout version=$seekVersion " +
                            "state=${finalState ?: lastState ?: "unknown"} " +
                            "position=${(finalPos?.first ?: lastPositionMs)?.let { msToTimeString(it) } ?: "unknown"} " +
                            "target=${msToTimeString(expectedPositionMs)} elapsed=${elapsedMs}ms"
                    )
                    return null
                }
            }

            delay(200)
        }

        return null
    }

    fun seekBy(deltaMs: Long) {
        if (controlState != CastControlState.READY_PLAYING) return
        seekTo(currentPositionMs + deltaMs)
    }

    fun browseNext() = browseBy(1)

    fun browsePrevious() = browseBy(-1)

    private fun browseBy(delta: Int) {
        if (playlist.isEmpty()) return
        if (controlState != CastControlState.READY_PAUSED &&
            controlState != CastControlState.BROWSING &&
            controlState != CastControlState.ERROR) {
            return
        }

        val generation = nextRequestGeneration()
        currentIndex = (currentIndex + delta + playlist.size) % playlist.size
        val path = playlist[currentIndex]
        val title = playlistTitles.getOrElse(currentIndex) {
            File(path.removePrefix("file://")).nameWithoutExtension
        }

        currentTitle = title
        currentVideoPath = path
        currentPositionMs = 0L
        durationMs = 0L
        desiredPositionMs = 0L
        desiredPositionExplicit = true
        isPlaying = false
        isLoadingTrack = false
        controlState = CastControlState.BROWSING
        trace("CAST browse generation=$generation index=$currentIndex title=$title")
        notifyStateChangedAsync()

        scope.launch {
            val localDuration = probeMedia(path).durationMs ?: 0L
            if (generation == latestRequestGeneration &&
                controlState == CastControlState.BROWSING &&
                currentVideoPath == path) {
                durationMs = localDuration
                val upperBound = localDuration.takeIf { it > 0L } ?: Long.MAX_VALUE
                currentPositionMs = currentPositionMs.coerceIn(0L, upperBound)
                notifyStateChanged()
            }
        }
    }

    private fun beginBrowseAtPosition(posMs: Long) {
        if (currentVideoPath.isEmpty()) return
        if (controlState == CastControlState.READY_PAUSED) {
            val generation = nextRequestGeneration()
            trace("CAST browse current generation=$generation title=$currentTitle")
        }
        currentPositionMs = posMs
        desiredPositionMs = posMs
        desiredPositionExplicit = true
        isPlaying = false
        isLoadingTrack = false
        controlState = CastControlState.BROWSING
        notifyStateChangedAsync()
    }

    private suspend fun confirmCurrentMediaState(reason: String) {
        val expectedUrl = confirmedVideoUrl
        if (expectedUrl.isEmpty() || confirmedVideoPath.isEmpty()) {
            isLoadingTrack = false
            controlState = CastControlState.ERROR
            notifyStateChanged()
            return
        }

        val deadline = System.currentTimeMillis() + 10_000L
        var lastState: String? = null
        while (isConnected && System.currentTimeMillis() < deadline) {
            val uri = getCurrentMediaUriOrNull()
            val state = getTransportStateOrNull()
            val pos = getPositionInfo()
            if (state != null && state != lastState) {
                trace("CAST wait[$reason] state=$state")
                lastState = state
            }

            val uriMatches = uri.isNullOrBlank() || mediaUrisMatch(uri, expectedUrl)
            if (uriMatches && state in setOf("PLAYING", "PAUSED_PLAYBACK") &&
                pos != null && pos.second > 0L) {
                currentPositionMs = pos.first
                durationMs = pos.second
                isPlaying = state == "PLAYING"
                isLoadingTrack = false
                controlState = if (isPlaying) {
                    CastControlState.READY_PLAYING
                } else {
                    CastControlState.READY_PAUSED
                }
                notifyStateChanged()
                return
            }
            delay(250)
        }

        trace("CAST wait[$reason] timeout state=${lastState ?: "unknown"}")
        isPlaying = false
        isLoadingTrack = false
        controlState = CastControlState.ERROR
        notifyStateChanged()
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
        controlState = if (isConnected) CastControlState.IDLE else CastControlState.DISCONNECTED
        confirmedVideoPath = ""
        confirmedVideoUrl = ""
        nextRequestGeneration()
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
        isLoadingTrack = false
        controlState = CastControlState.DISCONNECTED
        confirmedVideoPath = ""
        confirmedVideoUrl = ""
        nextRequestGeneration()
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

    private fun getCurrentMediaUriOrNull(): String? {
        val device = connectedDevice ?: return null
        return try {
            val response = sendSoap(device.controlUrl, "GetMediaInfo", "") ?: return null
            extractXmlTag(response, "CurrentURI")?.takeIf { it.isNotBlank() }
        } catch (_: Exception) {
            null
        }
    }

    private fun mediaUrisMatch(actual: String, expected: String): Boolean {
        fun normalize(value: String): String {
            val trimmed = value.trim().replace("&amp;", "&")
            return runCatching { URLDecoder.decode(trimmed, "UTF-8") }.getOrDefault(trimmed)
        }
        return normalize(actual) == normalize(expected)
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
