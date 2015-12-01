package com.icbc.pdlp

/**
  * Created by ConnorWeng on 2015/12/1.
  */
case class LogRecord(appId: String, mid: String, sid: String, timestamp: String, page: String,
                     event: String, menu: String, other: String) {}
