package com.icbc.pdlp

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
  * Created by ConnorWeng on 2015/11/27.
  */
class MenuLogDealer(consumer: LogConsumer, sc: SparkContext) extends LogDealer(consumer, sc) {
  override def accept(material: RDD[LogRecord]): Unit = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val table = material.toDF().registerTempTable("records")

    sqlContext.sql("""
      select
        appId, mid, menu, page,
        count(1) clicks, sum(other) duration,
        date, min(timestamp) start_time, max(timestamp) end_time
      from records
      where event = 'duration' and menu != ''
      group by appId,mid,date,menu,page
      """.stripMargin).show(false)
  }
}
