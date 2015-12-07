package com.icbc.pdlp

import com.icbc.pdlp.LogRecord.String2LogRecord
import org.apache.spark.rdd.RDD

/**
  * Created by ConnorWeng on 2015/12/1.
  */
class MenuLogMachine extends LogMachine {
  override def process(material: RDD[LogRecord]): RDD[LogRecord] = {
    val remainRdd = material.filter(record => record.event != "pageload" && (record.event != "click" || record.menu.isEmpty))
    val menuRdd = material.filter(record => record.event == "pageload" || (record.event == "click" && !record.menu.isEmpty))
      .map(record =>
        (record.appId, record.mid, record.sid) -> (record.page, record.event, record.timestamp, record.menu, record.other))
      .groupByKey()
      .flatMap(t => {
        var result = List[LogRecord]()
        val sorted = t._2.toList.sortBy(_._3)
        val iterator = sorted.iterator
        var nextMenu = ""
        while (iterator.hasNext) {
          val v = iterator.next()
          if (v._2 == "click") {
            nextMenu = v._4
          }
          result = result ::: List(s"${t._1._1},${t._1._2},${t._1._3},${v._3},${v._1},${v._2},$nextMenu,${v._5}".mkLogRecord)
          if (v._2 == "pageload" && !nextMenu.isEmpty) {
            nextMenu = ""
          }
        }
        result
      })
    menuRdd.union(remainRdd).sortBy(_.timestamp)
  }
}
