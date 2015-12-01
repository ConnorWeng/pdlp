package com.icbc.pdlp

import org.apache.spark.rdd.RDD

/**
  * Created by ConnorWeng on 2015/11/27.
  */
class MenuLogDealer(consumer: LogConsumer) extends LogDealer(consumer) {
  override def accept(material: RDD[LogRecord]): Unit = {
  }
}
