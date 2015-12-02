package com.icbc.pdlp

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by ConnorWeng on 2015/11/27.
  */
class PageLogDealer(consumer: LogConsumer, sc: SparkContext) extends LogDealer(consumer, sc) {
  override def accept(material: RDD[LogRecord]): Unit = {

  }
}
