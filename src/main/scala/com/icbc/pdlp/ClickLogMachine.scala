package com.icbc.pdlp

import org.apache.spark.rdd.RDD
import org.json4s.native.JsonParser

/**
  * Created by ConnorWeng on 2015/12/3.
  */
class ClickLogMachine extends LogMachine {
  override def process(material: RDD[LogRecord]): RDD[LogRecord] = {
    material.map(record => {
      if (record.event == "click") {
        val otherValue = JsonParser.parse(record.other)
        val srcElement = (otherValue \ "srcElement").values
        record.other = srcElement.toString
      }
      record
    })
  }
}
