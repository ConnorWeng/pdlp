package com.icbc.pdlp

import org.apache.spark.rdd.RDD

/**
  * Created by ConnorWeng on 2015/11/27.
  */
abstract class LogConsumer {
  def consume(production: RDD[LogRecord])
}
