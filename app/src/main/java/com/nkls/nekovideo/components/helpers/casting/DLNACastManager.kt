package com.nkls.nekovideo.components.helpers

import android.content.Context
import android.content.Intent
import android.media.MediaExtractor
import android.media.MediaFormat
import android.media.MediaMetadataRetriever
import android.net.wifi.WifiManager
import android.util.Log
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
            while (isConnected) {
                try {
                    val pos = getPositionInfo()
                    if (pos != null) {
                        currentPositionMs = pos.first
                        durationMs = pos.second
                        val transportState = getTransportState()
                        isPlaying = transportState == "PLAYING"

                        // Auto-advance when video ends naturally (PLAYING → STOPPED/NO_MEDIA_PRESENT)
                        // Guard isLoadingTrack: Smart TVs briefly enter STOPPED during SetAVTransportURI
                        // which would otherwise trigger a spurious next() and skip the intended video.
                        if (wasPlaying && !isPlaying && !stoppedByUser && !isLoadingTrack && playlist.size > 1
                            && transportState != "PAUSED_PLAYBACK") {
                            withContext(Dispatchers.Main) { next() }
                        }

                        wasPlaying = isPlaying
                        withContext(Dispatchers.Main) {
                            onStateChanged?.invoke()
                            onServiceStateChanged?.invoke()
                        }
                    }
                } catch (_: Exception) {
                }
                delay(500)
            }
        }
    }

    // ── Casting ──────────────────────────────────────────────────────────────

    fun castVideo(videoPath: String, videoTitle: String) {
        playlist = listOf(videoPath)
        playlistTitles = listOf(videoTitle)
        currentIndex = 0
        prepareServer()
        loadAndPlay(videoPath, videoTitle)
    }

    fun castPlaylist(videosPaths: List<String>, videosTitles: List<String>, startIndex: Int = 0) {
        playlist = videosPaths
        playlistTitles = videosTitles
        currentIndex = startIndex
        prepareServer()
        val path = videosPaths.getOrElse(startIndex) { return }
        val title = videosTitles.getOrElse(startIndex) { File(path.removePrefix("file://")).nameWithoutExtension }
        loadAndPlay(path, title)
    }

    private fun prepareServer() {
        if (videoServer == null) {
            videoServer = LocalVideoServer(context, 8080).also { it.start() }
        }
        videoServer!!.clearVideos()

        playlist.forEach { path ->
            if (path.startsWith("locked://")) {
                val filePath = path.removePrefix("locked://")
                val xorKey = LockedPlaybackSession.getXorKeyForFile(filePath)
                val obfuscatedName = File(filePath).name
                val originalName = LockedPlaybackSession.getOriginalName(obfuscatedName) ?: obfuscatedName
                if (xorKey != null) {
                    videoServer!!.addLockedVideo(filePath, xorKey, originalName)
                } else {
                    videoServer!!.addVideo(filePath)
                }
            } else {
                videoServer!!.addVideo(path.removePrefix("file://"))
            }
        }
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
        val videoLevel: Int? = null
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
        val extractor = MediaExtractor()
        try {
            extractor.setDataSource(file.absolutePath)
            for (i in 0 until extractor.trackCount) {
                val format = extractor.getTrackFormat(i)
                val mime = format.getString(MediaFormat.KEY_MIME) ?: continue
                if (mime.startsWith("video/") && videoMime == null) {
                    videoMime = mime
                    if (format.containsKey(MediaFormat.KEY_PROFILE)) videoProfile = format.getInteger(MediaFormat.KEY_PROFILE)
                    if (format.containsKey(MediaFormat.KEY_LEVEL)) videoLevel = format.getInteger(MediaFormat.KEY_LEVEL)
                }
                if (mime.startsWith("audio/") && audioMime == null) audioMime = mime
            }
        } catch (e: Exception) {
            Log.w(tag, "MediaExtractor failed for ${file.name}: ${e.message}")
        } finally {
            extractor.release()
        }

        return DlnaMediaInfo(file.length(), durationMs, width, height, videoMime, audioMime, videoProfile, videoLevel)
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
            info.size?.takeIf { it > 0 }?.let { append(" size=\\\"$it\\\"") }
            info.durationMs?.takeIf { it > 0 }?.let { append(" duration=\\\"${formatDlnaDuration(it)}\\\"") }
            if (info.width != null && info.height != null && info.width!! > 0 && info.height!! > 0) {
                append(" resolution=\\\"${info.width}x${info.height}\\\"")
            }
        }
        Log.d(tag, "DLNA media: mime=$mimeType video=${info.videoMime} profile=${info.videoProfile} level=${info.videoLevel} audio=${info.audioMime} size=${info.size} duration=${info.durationMs} resolution=${info.width}x${info.height} dlnaProfile=$profileName")
        val didl = """<DIDL-Lite xmlns="urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:upnp="urn:schemas-upnp-org:metadata-1-0/upnp/"><item id="0" parentID="-1" restricted="false"><dc:title>${title.escapeXml()}</dc:title><upnp:class>object.item.videoItem</upnp:class><res protocolInfo="$protocolInfo"$attributes>${url.escapeXml()}</res></item></DIDL-Lite>"""
        return didl.escapeXml()
    }

    private fun loadAndPlay(videoPath: String, videoTitle: String) {
        val device = connectedDevice ?: return
        currentTitle = videoTitle
        currentVideoPath = videoPath
        stoppedByUser = false
        isLoadingTrack = true
        scope.launch {
            try {
                val url = videoUrlFor(videoPath)
                val mime = mimeTypeFor(videoPath)
                val mediaInfo = probeMedia(videoPath)
                val metadata = buildDIDLMetadata(videoTitle, url, mime, mediaInfo)
                sendSoap(device.controlUrl, "SetAVTransportURI",
                    "<CurrentURI>${url.escapeXml()}</CurrentURI><CurrentURIMetaData>$metadata</CurrentURIMetaData>")
                delay(500)
                sendSoap(device.controlUrl, "Play", "<Speed>1</Speed>")
                isPlaying = true
                // Keep the flag set until the TV has had time to transition to PLAYING.
                // Smart TVs briefly report STOPPED during SetAVTransportURI; without this
                // guard the polling loop would fire next() and skip the intended video.
                delay(1500)
                isLoadingTrack = false
            } catch (e: Exception) {
                Log.e(tag, "loadAndPlay error", e)
                isLoadingTrack = false
            }
        }
    }

    // ── Playback controls ────────────────────────────────────────────────────

    fun play() {
        val device = connectedDevice ?: return
        scope.launch {
            sendSoap(device.controlUrl, "Play", "<Speed>1</Speed>")
            isPlaying = true
        }
    }

    fun pause() {
        val device = connectedDevice ?: return
        scope.launch {
            sendSoap(device.controlUrl, "Pause", "")
            isPlaying = false
        }
    }

    fun seekTo(posMs: Long) {
        val device = connectedDevice ?: return
        scope.launch {
            sendSoap(device.controlUrl, "Seek",
                "<Unit>REL_TIME</Unit><Target>${msToTimeString(posMs)}</Target>")
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

    private fun getTransportState(): String {
        val device = connectedDevice ?: return "STOPPED"
        return try {
            val response = sendSoap(device.controlUrl, "GetTransportInfo", "") ?: return "STOPPED"
            extractXmlTag(response, "CurrentTransportState") ?: "STOPPED"
        } catch (_: Exception) { "STOPPED" }
    }

    // ── SOAP ─────────────────────────────────────────────────────────────────

    private fun sendSoap(controlUrl: String, action: String, args: String): String? {
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
            val conn = (url.openConnection() as HttpURLConnection).apply {
                requestMethod = "POST"
                doOutput = true
                connectTimeout = 3000
                readTimeout = 3000
                setRequestProperty("Content-Type", "text/xml; charset=\"utf-8\"")
                setRequestProperty("SOAPAction",
                    "\"urn:schemas-upnp-org:service:AVTransport:1#$action\"")
            }
            val startedAt = System.currentTimeMillis()
            Log.d(tag, "SOAP -> $action url=$controlUrl")
            conn.outputStream.write(soap.toByteArray(Charsets.UTF_8))
            val status = conn.responseCode
            val response = try {
                conn.inputStream.bufferedReader().readText()
            } catch (_: Exception) {
                conn.errorStream?.bufferedReader()?.readText()
            }
            Log.d(tag, "SOAP <- $action HTTP $status in ${System.currentTimeMillis() - startedAt}ms body=${response?.take(1000)}")
            conn.disconnect()
            response
        } catch (e: Exception) {
            Log.w(tag, "SOAP $action failed: ${e.message}")
            null
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

    private fun String.escapeXml() = replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
}
