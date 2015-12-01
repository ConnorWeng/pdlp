package com.icbc.pdlp

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD

/**
  * Created by ConnorWeng on 2015/11/27.
  */
class DayLogMachine extends LogMachine {
  private val dateFormat = new SimpleDateFormat("yyyyMMdd")

  override def process(material: RDD[String]): RDD[String] = {
    material
  }

  private def formatDate(timestamp: Long): String = {
    dateFormat.format(new Date(timestamp))
  }
}
