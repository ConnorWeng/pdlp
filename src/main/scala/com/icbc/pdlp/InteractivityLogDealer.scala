package com.icbc.pdlp

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
  * Created by Connor on 12/28/15.
  */
class InteractivityLogDealer(consumer: LogConsumer, sc: SparkContext) extends LogDealer(consumer, sc) {
  override def accept(material: RDD[LogRecord]): Unit = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    consumer.consume(material.toDF(), "interactivity")
  }
}
