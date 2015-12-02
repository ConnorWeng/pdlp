package com.icbc.pdlp

import java.lang
import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD

/**
  * Created by ConnorWeng on 2015/11/27.
  */
class DayLogMachine extends LogMachine {
  override def process(material: RDD[LogRecord]): RDD[LogRecord] = {
    material.map(record => {
      val dateFormat = new SimpleDateFormat("yyyyMMdd")
      record.date = dateFormat.format(lang.Long.parseLong(record.timestamp))
      record
    })
  }
}
