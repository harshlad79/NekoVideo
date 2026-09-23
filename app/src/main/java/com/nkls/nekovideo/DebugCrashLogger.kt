package com.nkls.nekovideo

import android.content.Context
import java.io.File
import java.io.PrintWriter
import java.io.StringWriter
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

object DebugCrashLogger {
    private const val FILE_NAME = "last_crash.txt"

    fun install(context: Context) {
        if (!BuildConfig.DEBUG) return
        val appContext = context.applicationContext
        val previous = Thread.getDefaultUncaughtExceptionHandler()

        Thread.setDefaultUncaughtExceptionHandler { thread, throwable ->
            try {
                val sw = StringWriter()
                throwable.printStackTrace(PrintWriter(sw))
                val timestamp = SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS Z", Locale.US).format(Date())
                File(appContext.filesDir, FILE_NAME).writeText(
                    "timestamp=$timestamp\nthread=${thread.name}\n\n$sw"
                )
            } catch (_: Throwable) {
                // Never interfere with the platform crash path.
            } finally {
                previous?.uncaughtException(thread, throwable)
            }
        }
    }

    fun read(context: Context): String? {
        if (!BuildConfig.DEBUG) return null
        val file = File(context.filesDir, FILE_NAME)
        return if (file.isFile) file.readText() else null
    }

    fun clear(context: Context) {
        File(context.filesDir, FILE_NAME).delete()
    }
}
