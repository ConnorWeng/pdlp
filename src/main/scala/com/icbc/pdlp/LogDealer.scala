package com.icbc.pdlp

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by ConnorWeng on 2015/11/27.
  */
abstract class LogDealer(consumer: LogConsumer, sc: SparkContext) {
  def accept(material: RDD[LogRecord]): Unit
}
