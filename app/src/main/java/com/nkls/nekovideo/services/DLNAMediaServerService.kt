package com.nkls.nekovideo

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.net.wifi.WifiManager
import android.os.IBinder
import android.util.Base64
import android.util.Log
import androidx.core.app.NotificationCompat
import androidx.core.content.ContextCompat
import fi.iki.elonen.NanoHTTPD
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.net.DatagramPacket
import java.net.Inet4Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.MulticastSocket
import java.net.NetworkInterface
import java.util.UUID

/**
 * Foreground UPnP/DLNA MediaServer advertised on the local network.
 *
 * The server intentionally exposes only folders and supported video files below
 * /storage/emulated/0. NekoVideo private folders marked with .nekovideo are hidden.
 */
class DLNAMediaServerService : Service() {

    private val serviceScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private var httpServer: MediaServerHttpServer? = null
    private var ssdpJob: Job? = null
    private var multicastLock: WifiManager.MulticastLock? = null
    private lateinit var uuid: String

    override fun onCreate() {
        super.onCreate()
        createNotificationChannel()
        startForeground(NOTIFICATION_ID, buildNotification())

        uuid = getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
            .getString(KEY_UUID, null)
            ?: UUID.randomUUID().toString().also {
                getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
                    .edit()
                    .putString(KEY_UUID, it)
                    .apply()
            }

        startMediaServer()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (!isEnabled(this)) {
            stopSelf()
            return START_NOT_STICKY
        }
        if (httpServer == null || ssdpJob?.isActive != true) {
            startMediaServer()
        }
        return START_STICKY
    }

    private fun startMediaServer() {
        if (httpServer == null) {
            try {
                httpServer = MediaServerHttpServer(HTTP_PORT, uuid).also {
                    it.start(NanoHTTPD.SOCKET_READ_TIMEOUT, false)
                }
                Log.d(TAG, "HTTP MediaServer started port=" + HTTP_PORT)
            } catch (e: Exception) {
                Log.e(TAG, "Unable to start HTTP MediaServer", e)
                httpServer?.stop()
                httpServer = null
                stopSelf()
                return
            }
        }

        if (ssdpJob?.isActive == true) return

        val wifiManager = applicationContext.getSystemService(Context.WIFI_SERVICE) as WifiManager
        multicastLock = wifiManager.createMulticastLock("NekoVideoDlnaMediaServer").apply {
            setReferenceCounted(false)
            acquire()
        }

        ssdpJob = serviceScope.launch {
            runSsdp()
        }
    }

    private suspend fun runSsdp() {
        val group = InetAddress.getByName(SSDP_HOST)
        var socket: MulticastSocket? = null
        try {
            val iface = preferredNetworkInterface()
            socket = MulticastSocket(null).apply {
                reuseAddress = true
                bind(InetSocketAddress(SSDP_PORT))
                soTimeout = 1000
                timeToLive = 2
                if (iface != null) {
                    networkInterface = iface
                    joinGroup(InetSocketAddress(group, SSDP_PORT), iface)
                } else {
                    @Suppress("DEPRECATION")
                    joinGroup(group)
                }
            }

            sendAllAdvertisements(socket, alive = true)
            delay(150)
            sendAllAdvertisements(socket, alive = true)

            var nextAliveAt = System.currentTimeMillis() + ALIVE_REFRESH_MS
            val buffer = ByteArray(8192)

            while (isActive) {
                if (System.currentTimeMillis() >= nextAliveAt) {
                    sendAllAdvertisements(socket, alive = true)
                    nextAliveAt = System.currentTimeMillis() + ALIVE_REFRESH_MS
                }

                try {
                    val packet = DatagramPacket(buffer, buffer.size)
                    socket.receive(packet)
                    val message = String(packet.data, packet.offset, packet.length, Charsets.UTF_8)
                    if (message.startsWith("M-SEARCH", ignoreCase = true) &&
                        message.contains("ssdp:discover", ignoreCase = true)
                    ) {
                        respondToSearch(socket, packet, message)
                    }
                } catch (_: java.net.SocketTimeoutException) {
                }
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            Log.e(TAG, "SSDP server stopped by error", e)
        } finally {
            socket?.let {
                runCatching { sendAllAdvertisements(it, alive = false) }
                runCatching { it.close() }
            }
        }
    }

    private fun respondToSearch(socket: MulticastSocket, request: DatagramPacket, message: String) {
        val st = message.lineSequence()
            .firstOrNull { it.startsWith("ST:", ignoreCase = true) }
            ?.substringAfter(":")
            ?.trim()
            ?: return

        val targets = when {
            st.equals("ssdp:all", ignoreCase = true) -> advertisementTargets()
            else -> advertisementTargets().filter { it.first.equals(st, ignoreCase = true) }
        }

        targets.forEach { (target, usn) ->
            val location = deviceDescriptionUrl() ?: return@forEach
            val response = buildString {
                append("HTTP/1.1 200 OK\r\n")
                append("CACHE-CONTROL: max-age=" + MAX_AGE_SECONDS + "\r\n")
                append("EXT:\r\n")
                append("LOCATION: " + location + "\r\n")
                append("SERVER: Android UPnP/1.1 NekoVideo/1.0\r\n")
                append("ST: " + target + "\r\n")
                append("USN: " + usn + "\r\n")
                append("\r\n")
            }.toByteArray(Charsets.UTF_8)

            runCatching {
                socket.send(
                    DatagramPacket(
                        response,
                        response.size,
                        request.address,
                        request.port
                    )
                )
            }.onFailure {
                Log.w(TAG, "M-SEARCH response failed: " + it.message)
            }
        }
    }

    private fun sendAllAdvertisements(socket: MulticastSocket, alive: Boolean) {
        val location = deviceDescriptionUrl()
        val group = InetAddress.getByName(SSDP_HOST)
        advertisementTargets().forEach { (target, usn) ->
            val packetText = buildString {
                append("NOTIFY * HTTP/1.1\r\n")
                append("HOST: " + SSDP_HOST + ":" + SSDP_PORT + "\r\n")
                if (alive) {
                    append("CACHE-CONTROL: max-age=" + MAX_AGE_SECONDS + "\r\n")
                    if (location != null) append("LOCATION: " + location + "\r\n")
                    append("SERVER: Android UPnP/1.1 NekoVideo/1.0\r\n")
                }
                append("NT: " + target + "\r\n")
                append("NTS: " + if (alive) "ssdp:alive" else "ssdp:byebye" + "\r\n")
                append("USN: " + usn + "\r\n")
                append("\r\n")
            }.toByteArray(Charsets.UTF_8)

            runCatching {
                socket.send(DatagramPacket(packetText, packetText.size, group, SSDP_PORT))
            }
        }
        Log.d(TAG, "SSDP " + if (alive) "alive" else "byebye")
    }

    private fun advertisementTargets(): List<Pair<String, String>> {
        val deviceUuid = "uuid:" + uuid
        return listOf(
            "upnp:rootdevice" to (deviceUuid + "::upnp:rootdevice"),
            deviceUuid to deviceUuid,
            MEDIA_SERVER_TYPE to (deviceUuid + "::" + MEDIA_SERVER_TYPE),
            CONTENT_DIRECTORY_TYPE to (deviceUuid + "::" + CONTENT_DIRECTORY_TYPE),
            CONNECTION_MANAGER_TYPE to (deviceUuid + "::" + CONNECTION_MANAGER_TYPE)
        )
    }

    private fun deviceDescriptionUrl(): String? {
        val ip = preferredIpv4Address()?.hostAddress ?: return null
        return "http://" + ip + ":" + HTTP_PORT + "/device.xml"
    }

    private fun preferredNetworkInterface(): NetworkInterface? {
        val address = preferredIpv4Address() ?: return null
        return runCatching { NetworkInterface.getByInetAddress(address) }.getOrNull()
    }

    private fun preferredIpv4Address(): Inet4Address? {
        return runCatching {
            val candidates = mutableListOf<Pair<NetworkInterface, Inet4Address>>()
            val interfaces = NetworkInterface.getNetworkInterfaces()
            while (interfaces.hasMoreElements()) {
                val iface = interfaces.nextElement()
                if (!iface.isUp || iface.isLoopback) continue
                val addrs = iface.inetAddresses
                while (addrs.hasMoreElements()) {
                    val address = addrs.nextElement()
                    if (address is Inet4Address &&
                        !address.isLoopbackAddress &&
                        address.isSiteLocalAddress
                    ) {
                        candidates += iface to address
                    }
                }
            }
            candidates.firstOrNull { (iface, _) -> iface.name.startsWith("wlan") }?.second
                ?: candidates.firstOrNull()?.second
        }.getOrNull()
    }

    private fun buildNotification(): Notification {
        val openApp = PendingIntent.getActivity(
            this,
            0,
            Intent(this, MainActivity::class.java).apply {
                flags = Intent.FLAG_ACTIVITY_SINGLE_TOP or Intent.FLAG_ACTIVITY_CLEAR_TOP
            },
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )

        return NotificationCompat.Builder(this, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_notification)
            .setContentTitle(getString(R.string.dlna_server_notification_title))
            .setContentText(getString(R.string.dlna_server_notification_text))
            .setContentIntent(openApp)
            .setOngoing(true)
            .setOnlyAlertOnce(true)
            .setCategory(NotificationCompat.CATEGORY_SERVICE)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .build()
    }

    private fun createNotificationChannel() {
        val channel = NotificationChannel(
            CHANNEL_ID,
            getString(R.string.dlna_server_notification_channel),
            NotificationManager.IMPORTANCE_LOW
        ).apply {
            setShowBadge(false)
        }
        (getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager)
            .createNotificationChannel(channel)
    }

    override fun onDestroy() {
        ssdpJob?.cancel()
        ssdpJob = null
        httpServer?.stop()
        httpServer = null
        runCatching {
            multicastLock?.takeIf { it.isHeld }?.release()
        }
        multicastLock = null
        serviceScope.cancel()
        super.onDestroy()
    }

    override fun onBind(intent: Intent?): IBinder? = null

    companion object {
        private const val TAG = "DLNAMediaServer"
        private const val PREFS_NAME = "nekovideo_settings"
        private const val KEY_ENABLED = "dlna_media_server_enabled"
        private const val KEY_UUID = "dlna_media_server_uuid"

        private const val CHANNEL_ID = "dlna_media_server_channel"
        private const val NOTIFICATION_ID = 2002

        private const val HTTP_PORT = 8192
        private const val SSDP_PORT = 1900
        private const val SSDP_HOST = "239.255.255.250"
        private const val MAX_AGE_SECONDS = 1800
        private const val ALIVE_REFRESH_MS = 15L * 60L * 1000L

        private const val MEDIA_SERVER_TYPE = "urn:schemas-upnp-org:device:MediaServer:1"
        private const val CONTENT_DIRECTORY_TYPE = "urn:schemas-upnp-org:service:ContentDirectory:1"
        private const val CONNECTION_MANAGER_TYPE = "urn:schemas-upnp-org:service:ConnectionManager:1"

        fun isEnabled(context: Context): Boolean =
            context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
                .getBoolean(KEY_ENABLED, false)

        fun setEnabled(context: Context, enabled: Boolean) {
            context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
                .edit()
                .putBoolean(KEY_ENABLED, enabled)
                .apply()

            if (enabled) {
                ContextCompat.startForegroundService(
                    context,
                    Intent(context, DLNAMediaServerService::class.java)
                )
            } else {
                context.stopService(Intent(context, DLNAMediaServerService::class.java))
            }
        }

        fun startIfEnabled(context: Context) {
            if (isEnabled(context)) {
                ContextCompat.startForegroundService(
                    context,
                    Intent(context, DLNAMediaServerService::class.java)
                )
            }
        }
    }
}

private class MediaServerHttpServer(
    port: Int,
    private val uuid: String
) : NanoHTTPD(port) {

    private val root = File("/storage/emulated/0")
    private val supportedVideoExtensions = setOf(
        "mp4", "m4v", "mkv", "webm", "avi", "mov", "3gp", "3gpp", "ts", "m2ts", "mpeg", "mpg"
    )

    override fun serve(session: IHTTPSession): Response {
        return try {
            when {
                session.method == Method.GET && session.uri == "/device.xml" ->
                    xml(deviceDescription())

                session.method == Method.GET && session.uri == "/content-directory.xml" ->
                    xml(contentDirectoryScpd())

                session.method == Method.GET && session.uri == "/connection-manager.xml" ->
                    xml(connectionManagerScpd())

                session.method == Method.POST && session.uri == "/control/content-directory" ->
                    serveContentDirectory(session)

                session.method == Method.POST && session.uri == "/control/connection-manager" ->
                    serveConnectionManager(session)

                session.uri.startsWith("/media/") ->
                    serveMedia(session)

                else -> newFixedLengthResponse(
                    Response.Status.NOT_FOUND,
                    MIME_PLAINTEXT,
                    "Not found"
                )
            }
        } catch (e: Exception) {
            Log.e("DLNAMediaServer", "HTTP request failed " + session.uri, e)
            newFixedLengthResponse(
                Response.Status.INTERNAL_ERROR,
                MIME_PLAINTEXT,
                "Server error"
            )
        }
    }

    private fun serveContentDirectory(session: IHTTPSession): Response {
        val request = readPostBody(session)
        val action = soapAction(session)

        return when (action) {
            "Browse" -> {
                val objectId = extractTag(request, "ObjectID") ?: "0"
                val browseFlag = extractTag(request, "BrowseFlag") ?: "BrowseDirectChildren"
                val startIndex = extractTag(request, "StartingIndex")?.toIntOrNull() ?: 0
                val requestedCount = extractTag(request, "RequestedCount")?.toIntOrNull() ?: 0
                val allEntries = if (browseFlag == "BrowseMetadata") {
                    listOfNotNull(metadataForObject(objectId))
                } else {
                    childrenForObject(objectId)
                }
                val entries = allEntries
                    .drop(startIndex.coerceAtLeast(0))
                    .let { list ->
                        if (requestedCount > 0) list.take(requestedCount) else list
                    }
                val didl = didl(entries.joinToString(""))
                soap(
                    "Browse",
                    "<Result>" + escapeXml(didl) + "</Result>" +
                        "<NumberReturned>" + entries.size + "</NumberReturned>" +
                        "<TotalMatches>" + allEntries.size + "</TotalMatches>" +
                        "<UpdateID>1</UpdateID>"
                )
            }

            "GetSearchCapabilities" -> soap(
                "GetSearchCapabilities",
                "<SearchCaps></SearchCaps>"
            )

            "GetSortCapabilities" -> soap(
                "GetSortCapabilities",
                "<SortCaps>dc:title</SortCaps>"
            )

            "GetSystemUpdateID" -> soap(
                "GetSystemUpdateID",
                "<Id>1</Id>"
            )

            else -> soapFault(401, "Invalid Action")
        }
    }

    private fun serveConnectionManager(session: IHTTPSession): Response {
        return when (soapAction(session)) {
            "GetProtocolInfo" -> soap(
                "GetProtocolInfo",
                "<Source>http-get:*:video/mp4:*,http-get:*:video/x-matroska:*,http-get:*:video/webm:*,http-get:*:video/*:*</Source>" +
                    "<Sink></Sink>",
                serviceType = "urn:schemas-upnp-org:service:ConnectionManager:1"
            )

            "GetCurrentConnectionIDs" -> soap(
                "GetCurrentConnectionIDs",
                "<ConnectionIDs></ConnectionIDs>",
                serviceType = "urn:schemas-upnp-org:service:ConnectionManager:1"
            )

            "GetCurrentConnectionInfo" -> soap(
                "GetCurrentConnectionInfo",
                "<RcsID>-1</RcsID><AVTransportID>-1</AVTransportID><ProtocolInfo></ProtocolInfo>" +
                    "<PeerConnectionManager></PeerConnectionManager><PeerConnectionID>-1</PeerConnectionID>" +
                    "<Direction>Output</Direction><Status>Unknown</Status>",
                serviceType = "urn:schemas-upnp-org:service:ConnectionManager:1"
            )

            else -> soapFault(401, "Invalid Action")
        }
    }

    private fun childrenForObject(objectId: String): List<String> {
        if (objectId == "0") {
            return listOf(containerXml("internal", "0", "Internal storage", childCount(root)))
        }

        val directory = when {
            objectId == "internal" -> root
            objectId.startsWith("d:") -> decodeFile(objectId.removePrefix("d:"))
            else -> null
        } ?: return emptyList()

        if (!isAllowedDirectory(directory)) return emptyList()

        return directory.listFiles()
            ?.asSequence()
            ?.filter { isVisibleEntry(it) }
            ?.sortedWith(compareBy<File>({ !it.isDirectory }, { it.name.lowercase() }))
            ?.map { file ->
                if (file.isDirectory) {
                    containerXml(
                        "d:" + encode(file.absolutePath),
                        objectId,
                        file.name,
                        childCount(file)
                    )
                } else {
                    itemXml(file, objectId)
                }
            }
            ?.toList()
            ?: emptyList()
    }

    private fun metadataForObject(objectId: String): String? {
        return when {
            objectId == "0" -> containerXml("0", "-1", "NekoVideo", 1)
            objectId == "internal" -> containerXml("internal", "0", "Internal storage", childCount(root))
            objectId.startsWith("d:") -> {
                val file = decodeFile(objectId.removePrefix("d:")) ?: return null
                if (!isAllowedDirectory(file)) return null
                containerXml(objectId, parentId(file.parentFile), file.name, childCount(file))
            }
            objectId.startsWith("f:") -> {
                val file = decodeFile(objectId.removePrefix("f:")) ?: return null
                if (!isAllowedVideo(file)) return null
                itemXml(file, parentId(file.parentFile))
            }
            else -> null
        }
    }

    private fun parentId(parent: File?): String {
        if (parent == null) return "0"
        return if (sameFile(parent, root)) "internal" else "d:" + encode(parent.absolutePath)
    }

    private fun childCount(directory: File): Int =
        directory.listFiles()?.count { isVisibleEntry(it) } ?: 0

    private fun isVisibleEntry(file: File): Boolean {
        if (file.name.startsWith(".")) return false
        if (file.isDirectory) {
            if (!isAllowedDirectory(file)) return false
            if (File(file, ".nekovideo").exists()) return false
            return true
        }
        return isAllowedVideo(file)
    }

    private fun isAllowedDirectory(file: File): Boolean =
        file.isDirectory && isInsideRoot(file)

    private fun isAllowedVideo(file: File): Boolean =
        file.isFile &&
            isInsideRoot(file) &&
            file.extension.lowercase() in supportedVideoExtensions

    private fun isInsideRoot(file: File): Boolean {
        return runCatching {
            val rootPath = root.canonicalPath
            val filePath = file.canonicalPath
            filePath == rootPath || filePath.startsWith(rootPath + File.separator)
        }.getOrDefault(false)
    }

    private fun sameFile(a: File, b: File): Boolean =
        runCatching { a.canonicalPath == b.canonicalPath }.getOrDefault(false)

    private fun itemXml(file: File, parentId: String): String {
        val id = "f:" + encode(file.absolutePath)
        val url = mediaUrl(file)
        val mime = mimeType(file)
        val protocolInfo = "http-get:*:" + mime +
            ":DLNA.ORG_OP=01;DLNA.ORG_FLAGS=01700000000000000000000000000000"

        return "<item id=\"" + escapeXml(id) + "\" parentID=\"" + escapeXml(parentId) +
            "\" restricted=\"1\">" +
            "<dc:title>" + escapeXml(file.nameWithoutExtension) + "</dc:title>" +
            "<upnp:class>object.item.videoItem</upnp:class>" +
            "<res protocolInfo=\"" + escapeXml(protocolInfo) + "\" size=\"" + file.length() + "\">" +
            escapeXml(url) +
            "</res></item>"
    }

    private fun containerXml(id: String, parentId: String, title: String, childCount: Int): String {
        return "<container id=\"" + escapeXml(id) + "\" parentID=\"" + escapeXml(parentId) +
            "\" childCount=\"" + childCount + "\" restricted=\"1\" searchable=\"0\">" +
            "<dc:title>" + escapeXml(title) + "</dc:title>" +
            "<upnp:class>object.container.storageFolder</upnp:class>" +
            "</container>"
    }

    private fun didl(body: String): String =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<DIDL-Lite xmlns=\"urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/\"" +
            " xmlns:dc=\"http://purl.org/dc/elements/1.1/\"" +
            " xmlns:upnp=\"urn:schemas-upnp-org:metadata-1-0/upnp/\">" +
            body +
            "</DIDL-Lite>"

    private fun mediaUrl(file: File): String {
        val ip = localIpv4Address() ?: "127.0.0.1"
        return "http://" + ip + ":" + listeningPort + "/media/" + encode(file.absolutePath)
    }

    private fun serveMedia(session: IHTTPSession): Response {
        val token = session.uri.removePrefix("/media/")
        val file = decodeFile(token)
            ?.takeIf { isAllowedVideo(it) }
            ?: return newFixedLengthResponse(Response.Status.NOT_FOUND, MIME_PLAINTEXT, "Video not found")

        val fileSize = file.length()
        val rangeHeader = session.headers["range"]
        if (rangeHeader.isNullOrBlank()) {
            val stream = FileInputStream(file)
            return newFixedLengthResponse(
                Response.Status.OK,
                mimeType(file),
                stream,
                fileSize
            ).apply {
                addHeader("Accept-Ranges", "bytes")
                addHeader("contentFeatures.dlna.org", "DLNA.ORG_OP=01;DLNA.ORG_FLAGS=01700000000000000000000000000000")
                addHeader("transferMode.dlna.org", "Streaming")
            }
        }

        val match = Regex("bytes=(\\d*)-(\\d*)").find(rangeHeader)
            ?: return newFixedLengthResponse(Response.Status.RANGE_NOT_SATISFIABLE, MIME_PLAINTEXT, "")
        val requestedStart = match.groupValues[1].toLongOrNull() ?: 0L
        val requestedEnd = match.groupValues[2].toLongOrNull() ?: (fileSize - 1)
        val start = requestedStart.coerceIn(0L, (fileSize - 1).coerceAtLeast(0L))
        val end = requestedEnd.coerceIn(start, (fileSize - 1).coerceAtLeast(start))
        val length = end - start + 1

        val stream = FileInputStream(file)
        skipFully(stream, start)

        return newFixedLengthResponse(
            Response.Status.PARTIAL_CONTENT,
            mimeType(file),
            stream,
            length
        ).apply {
            addHeader("Content-Range", "bytes " + start + "-" + end + "/" + fileSize)
            addHeader("Accept-Ranges", "bytes")
            addHeader("Content-Length", length.toString())
            addHeader("contentFeatures.dlna.org", "DLNA.ORG_OP=01;DLNA.ORG_FLAGS=01700000000000000000000000000000")
            addHeader("transferMode.dlna.org", "Streaming")
        }
    }

    private fun skipFully(input: InputStream, bytes: Long) {
        var remaining = bytes
        while (remaining > 0) {
            val skipped = input.skip(remaining)
            if (skipped <= 0) {
                if (input.read() == -1) break
                remaining--
            } else {
                remaining -= skipped
            }
        }
    }

    private fun mimeType(file: File): String = when (file.extension.lowercase()) {
        "mp4", "m4v" -> "video/mp4"
        "mkv" -> "video/x-matroska"
        "webm" -> "video/webm"
        "avi" -> "video/x-msvideo"
        "mov" -> "video/quicktime"
        "3gp", "3gpp" -> "video/3gpp"
        "ts", "m2ts" -> "video/mp2t"
        "mpeg", "mpg" -> "video/mpeg"
        else -> "video/*"
    }

    private fun encode(value: String): String =
        Base64.encodeToString(
            value.toByteArray(Charsets.UTF_8),
            Base64.URL_SAFE or Base64.NO_WRAP or Base64.NO_PADDING
        )

    private fun decodeFile(value: String): File? =
        runCatching {
            File(
                String(
                    Base64.decode(
                        value,
                        Base64.URL_SAFE or Base64.NO_WRAP or Base64.NO_PADDING
                    ),
                    Charsets.UTF_8
                )
            )
        }.getOrNull()

    private fun soapAction(session: IHTTPSession): String =
        session.headers["soapaction"]
            ?.trim()
            ?.trim('"')
            ?.substringAfterLast("#")
            ?: ""

    private fun readPostBody(session: IHTTPSession): String {
        val files = mutableMapOf<String, String>()
        return runCatching {
            session.parseBody(files)
            files["postData"].orEmpty()
        }.getOrDefault("")
    }

    private fun extractTag(xml: String, name: String): String? {
        val regex = Regex(
            "<(?:[A-Za-z0-9_]+:)?" + Regex.escape(name) + "[^>]*>(.*?)</(?:[A-Za-z0-9_]+:)?" +
                Regex.escape(name) + ">",
            setOf(RegexOption.IGNORE_CASE, RegexOption.DOT_MATCHES_ALL)
        )
        return regex.find(xml)?.groupValues?.get(1)?.trim()
    }

    private fun soap(
        action: String,
        body: String,
        serviceType: String = "urn:schemas-upnp-org:service:ContentDirectory:1"
    ): Response {
        val responseBody =
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>" +
                "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\"" +
                " s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" +
                "<s:Body><u:" + action + "Response xmlns:u=\"" + serviceType + "\">" +
                body +
                "</u:" + action + "Response></s:Body></s:Envelope>"
        return xml(responseBody)
    }

    private fun soapFault(code: Int, description: String): Response {
        val body =
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>" +
                "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\"" +
                " s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">" +
                "<s:Body><s:Fault><faultcode>s:Client</faultcode><faultstring>UPnPError</faultstring>" +
                "<detail><UPnPError xmlns=\"urn:schemas-upnp-org:control-1-0\">" +
                "<errorCode>" + code + "</errorCode><errorDescription>" + escapeXml(description) +
                "</errorDescription></UPnPError></detail></s:Fault></s:Body></s:Envelope>"
        return newFixedLengthResponse(
            Response.Status.INTERNAL_ERROR,
            "text/xml; charset=\"utf-8\"",
            body
        )
    }

    private fun xml(body: String): Response =
        newFixedLengthResponse(Response.Status.OK, "text/xml; charset=\"utf-8\"", body)

    private fun deviceDescription(): String =
        "<?xml version=\"1.0\"?>" +
            "<root xmlns=\"urn:schemas-upnp-org:device-1-0\">" +
            "<specVersion><major>1</major><minor>0</minor></specVersion>" +
            "<device>" +
            "<deviceType>urn:schemas-upnp-org:device:MediaServer:1</deviceType>" +
            "<friendlyName>NekoVideo</friendlyName>" +
            "<manufacturer>NekoVideo</manufacturer>" +
            "<modelName>NekoVideo DLNA Media Server</modelName>" +
            "<UDN>uuid:" + escapeXml(uuid) + "</UDN>" +
            "<serviceList>" +
            "<service><serviceType>urn:schemas-upnp-org:service:ContentDirectory:1</serviceType>" +
            "<serviceId>urn:upnp-org:serviceId:ContentDirectory</serviceId>" +
            "<SCPDURL>/content-directory.xml</SCPDURL>" +
            "<controlURL>/control/content-directory</controlURL>" +
            "<eventSubURL>/event/content-directory</eventSubURL></service>" +
            "<service><serviceType>urn:schemas-upnp-org:service:ConnectionManager:1</serviceType>" +
            "<serviceId>urn:upnp-org:serviceId:ConnectionManager</serviceId>" +
            "<SCPDURL>/connection-manager.xml</SCPDURL>" +
            "<controlURL>/control/connection-manager</controlURL>" +
            "<eventSubURL>/event/connection-manager</eventSubURL></service>" +
            "</serviceList></device></root>"

    private fun contentDirectoryScpd(): String =
        "<?xml version=\"1.0\"?>" +
            "<scpd xmlns=\"urn:schemas-upnp-org:service-1-0\">" +
            "<specVersion><major>1</major><minor>0</minor></specVersion>" +
            "<actionList>" +
            actionXml("Browse", listOf(
                arg("ObjectID", "in", "A_ARG_TYPE_ObjectID"),
                arg("BrowseFlag", "in", "A_ARG_TYPE_BrowseFlag"),
                arg("Filter", "in", "A_ARG_TYPE_Filter"),
                arg("StartingIndex", "in", "A_ARG_TYPE_Index"),
                arg("RequestedCount", "in", "A_ARG_TYPE_Count"),
                arg("SortCriteria", "in", "A_ARG_TYPE_SortCriteria"),
                arg("Result", "out", "A_ARG_TYPE_Result"),
                arg("NumberReturned", "out", "A_ARG_TYPE_Count"),
                arg("TotalMatches", "out", "A_ARG_TYPE_Count"),
                arg("UpdateID", "out", "SystemUpdateID")
            )) +
            actionXml("GetSearchCapabilities", listOf(arg("SearchCaps", "out", "SearchCapabilities"))) +
            actionXml("GetSortCapabilities", listOf(arg("SortCaps", "out", "SortCapabilities"))) +
            actionXml("GetSystemUpdateID", listOf(arg("Id", "out", "SystemUpdateID"))) +
            "</actionList>" +
            "<serviceStateTable>" +
            stateXml("A_ARG_TYPE_ObjectID", "string") +
            stateXml("A_ARG_TYPE_BrowseFlag", "string") +
            stateXml("A_ARG_TYPE_Filter", "string") +
            stateXml("A_ARG_TYPE_Index", "ui4") +
            stateXml("A_ARG_TYPE_Count", "ui4") +
            stateXml("A_ARG_TYPE_SortCriteria", "string") +
            stateXml("A_ARG_TYPE_Result", "string") +
            stateXml("SearchCapabilities", "string") +
            stateXml("SortCapabilities", "string") +
            stateXml("SystemUpdateID", "ui4", sendEvents = true) +
            "</serviceStateTable></scpd>"

    private fun connectionManagerScpd(): String =
        "<?xml version=\"1.0\"?>" +
            "<scpd xmlns=\"urn:schemas-upnp-org:service-1-0\">" +
            "<specVersion><major>1</major><minor>0</minor></specVersion>" +
            "<actionList>" +
            actionXml("GetProtocolInfo", listOf(
                arg("Source", "out", "SourceProtocolInfo"),
                arg("Sink", "out", "SinkProtocolInfo")
            )) +
            actionXml("GetCurrentConnectionIDs", listOf(arg("ConnectionIDs", "out", "CurrentConnectionIDs"))) +
            actionXml("GetCurrentConnectionInfo", listOf(
                arg("ConnectionID", "in", "A_ARG_TYPE_ConnectionID"),
                arg("RcsID", "out", "A_ARG_TYPE_RcsID"),
                arg("AVTransportID", "out", "A_ARG_TYPE_AVTransportID"),
                arg("ProtocolInfo", "out", "A_ARG_TYPE_ProtocolInfo"),
                arg("PeerConnectionManager", "out", "A_ARG_TYPE_ConnectionManager"),
                arg("PeerConnectionID", "out", "A_ARG_TYPE_ConnectionID"),
                arg("Direction", "out", "A_ARG_TYPE_Direction"),
                arg("Status", "out", "A_ARG_TYPE_ConnectionStatus")
            )) +
            "</actionList>" +
            "<serviceStateTable>" +
            stateXml("SourceProtocolInfo", "string", sendEvents = true) +
            stateXml("SinkProtocolInfo", "string", sendEvents = true) +
            stateXml("CurrentConnectionIDs", "string", sendEvents = true) +
            stateXml("A_ARG_TYPE_ConnectionID", "i4") +
            stateXml("A_ARG_TYPE_RcsID", "i4") +
            stateXml("A_ARG_TYPE_AVTransportID", "i4") +
            stateXml("A_ARG_TYPE_ProtocolInfo", "string") +
            stateXml("A_ARG_TYPE_ConnectionManager", "string") +
            stateXml("A_ARG_TYPE_Direction", "string") +
            stateXml("A_ARG_TYPE_ConnectionStatus", "string") +
            "</serviceStateTable></scpd>"

    private fun actionXml(name: String, args: List<String>): String =
        "<action><name>" + name + "</name><argumentList>" +
            args.joinToString("") +
            "</argumentList></action>"

    private fun arg(name: String, direction: String, related: String): String =
        "<argument><name>" + name + "</name><direction>" + direction +
            "</direction><relatedStateVariable>" + related + "</relatedStateVariable></argument>"

    private fun stateXml(name: String, type: String, sendEvents: Boolean = false): String =
        "<stateVariable sendEvents=\"" + if (sendEvents) "yes" else "no" + "\"><name>" +
            name + "</name><dataType>" + type + "</dataType></stateVariable>"

    private fun localIpv4Address(): String? {
        return runCatching {
            val candidates = mutableListOf<Pair<NetworkInterface, Inet4Address>>()
            val interfaces = NetworkInterface.getNetworkInterfaces()
            while (interfaces.hasMoreElements()) {
                val iface = interfaces.nextElement()
                if (!iface.isUp || iface.isLoopback) continue
                val addresses = iface.inetAddresses
                while (addresses.hasMoreElements()) {
                    val address = addresses.nextElement()
                    if (address is Inet4Address &&
                        !address.isLoopbackAddress &&
                        address.isSiteLocalAddress
                    ) {
                        candidates += iface to address
                    }
                }
            }
            candidates.firstOrNull { it.first.name.startsWith("wlan") }?.second?.hostAddress
                ?: candidates.firstOrNull()?.second?.hostAddress
        }.getOrNull()
    }

    private fun escapeXml(value: String): String =
        value.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace("\"", "&quot;")
            .replace("'", "&apos;")
}
