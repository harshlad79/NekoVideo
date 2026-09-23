package com.nkls.nekovideo

import android.content.Context
import java.io.File
import java.io.PrintWriter
import java.io.StringWriter
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

object DebugCrashLogger {
    private const val PREFIX = "crash-"
    private const val SUFFIX = ".txt"
    private const val MAX_FILES = 10

    fun install(context: Context) {
        if (!BuildConfig.DEBUG) return
        val appContext = context.applicationContext
        val previous = Thread.getDefaultUncaughtExceptionHandler()

        Thread.setDefaultUncaughtExceptionHandler { thread, throwable ->
            try {
                val now = Date()
                val fileStamp = SimpleDateFormat("yyyyMMdd-HHmmss-SSS", Locale.US).format(now)
                val displayStamp = SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z", Locale.US).format(now)
                val sw = StringWriter()
                throwable.printStackTrace(PrintWriter(sw))

                File(appContext.filesDir, "$PREFIX$fileStamp$SUFFIX").writeText(
                    "timestamp=$displayStamp\nthread=${thread.name}\n\n$sw"
                )
                trimOldFiles(appContext)
            } catch (_: Throwable) {
                // Crash logging must never interfere with the platform crash path.
            } finally {
                previous?.uncaughtException(thread, throwable)
            }
        }
    }

    fun list(context: Context): List<File> {
        if (!BuildConfig.DEBUG) return emptyList()
        return context.filesDir.listFiles { file ->
            file.isFile && file.name.startsWith(PREFIX) && file.name.endsWith(SUFFIX)
        }?.sortedByDescending { it.name } ?: emptyList()
    }

    fun read(file: File): String? =
        runCatching { if (file.isFile) file.readText() else null }.getOrNull()

    fun delete(file: File): Boolean =
        runCatching { file.delete() }.getOrDefault(false)

    private fun trimOldFiles(context: Context) {
        list(context).drop(MAX_FILES).forEach { runCatching { it.delete() } }
    }
}
