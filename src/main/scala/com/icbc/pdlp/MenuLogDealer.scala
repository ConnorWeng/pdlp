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

    material.toDF().registerTempTable("records")
    val menuDF = sqlContext.sql("""
      select
        appId, mid, menu, page,
        count(1) clicks, sum(other) duration,
        date, min(timestamp) start_time, max(timestamp) end_time
      from records
      where event = 'duration' and menu != ''
      group by appId,mid,date,menu,page
      """.stripMargin)
    menuDF.registerTempTable("menu_view")
    consumer.consume(menuDF, "menu_view")

    material.toDF().registerTempTable("page_records")
    consumer.consume(sqlContext.sql(
      """
        |select
        |  r.appId, r.mid, r.page, r.other srcElement, count(1) clicks,
        |  r.date, min(r.timestamp) start_time, max(r.timestamp) end_time
        |from page_records r join menu_view v on r.appId = v.appId and r.mid = v.mid and r.page = v.page and r.date = v.date
        |where r.event = 'click'
        |group by r.appId,r.mid,r.date,r.page,r.other
      """.stripMargin), "page_view")
  }
}
