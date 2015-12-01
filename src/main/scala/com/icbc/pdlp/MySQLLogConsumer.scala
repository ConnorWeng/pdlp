package com.icbc.pdlp

import org.apache.spark.rdd.RDD

/**
  * Created by ConnorWeng on 2015/11/27.
  */
class MySQLLogConsumer extends LogConsumer() {
  override def consume(production: RDD[LogRecord]): Unit = {
  }
}
