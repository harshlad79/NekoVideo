package com.nkls.nekovideo

import android.content.Context
import android.os.Build
import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

object DebugTraceLogger {
    private const val FILE_NAME = "hang-trace.txt"
    private const val MAX_BYTES = 256 * 1024

    fun startSession(context: Context) {
        if (!BuildConfig.DEBUG) return
        log(context, "===== NekoVideo Debug Session =====")
        log(
            context,
            "BUILD git=${BuildConfig.GIT_SHA} version=${BuildConfig.VERSION_NAME} " +
                "versionCode=${BuildConfig.VERSION_CODE} buildType=${BuildConfig.BUILD_TYPE}"
        )
        log(
            context,
            "DEVICE manufacturer=${Build.MANUFACTURER} model=${Build.MODEL} " +
                "android=${Build.VERSION.RELEASE} sdk=${Build.VERSION.SDK_INT}"
        )
        log(context, "===================================")
    }

    @Synchronized
    fun log(context: Context, message: String) {
        if (!BuildConfig.DEBUG) return
        runCatching {
            val file = File(context.applicationContext.filesDir, FILE_NAME)
            if (file.exists() && file.length() > MAX_BYTES) {
                file.writeText("")
            }
            val stamp = SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z", Locale.US).format(Date())
            file.appendText("$stamp $message\n")
        }
    }

    fun file(context: Context): File? {
        if (!BuildConfig.DEBUG) return null
        return File(context.applicationContext.filesDir, FILE_NAME).takeIf { it.isFile && it.length() > 0 }
    }

    fun read(context: Context): String? =
        runCatching { file(context)?.readText() }.getOrNull()

    fun delete(context: Context): Boolean =
        runCatching {
            val file = File(context.applicationContext.filesDir, FILE_NAME)
            !file.exists() || file.delete()
        }.getOrDefault(false)
}
