package com.icbc.pdlp

/**
  * Created by ConnorWeng on 2015/12/1.
  */
case class LogRecord(appId: String, mid: String, sid: String, timestamp: String, page: String,
                     event: String, menu: String, other: String, date: String = "") {}

object LogRecord {
  implicit class String2LogRecord(line: String) {
    def mkLogRecord = {
      val parts = line.split(",", 8)
      new LogRecord(parts(0), parts(1), parts(2), parts(3), parts(4), parts(5), parts(6), parts(7))
    }
  }
}
