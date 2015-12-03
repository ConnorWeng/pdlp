package com.icbc.pdlp

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
  * Created by ConnorWeng on 2015/11/27.
  */
class PageLogDealer(consumer: LogConsumer, sc: SparkContext) extends LogDealer(consumer, sc) {
  override def accept(material: RDD[LogRecord]): Unit = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    material.toDF().registerTempTable("page_records")

    consumer.consume(sqlContext.sql(
      """
        |select
        |  appId, mid, page, other srcElement, count(1) clicks,
        |  date, min(timestamp) start_time, max(timestamp) end_time
        |from page_records
        |where event = 'click'
        |group by appId,mid,date,page,other
      """.stripMargin), "page_view")
  }
}
