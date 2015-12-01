package com.icbc.pdlp

import org.apache.spark.rdd.RDD
import com.icbc.pdlp.LogParser.String2LogRecord

/**
  * Created by ConnorWeng on 2015/11/26.
  */
class DurationLogMachine extends LogMachine {
  override def process(material: RDD[LogRecord]): RDD[LogRecord] = {
    val remainRdd = material.filter(record => record.event != "pageload" && record.event != "unload")
    val durationRdd = material.filter(record => record.event == "pageload" || record.event == "unload")
      .map(record => {
        ((record.appId, record.mid, record.sid, record.page) -> (record.event, record.timestamp, record.menu, record.other))
      })
      .groupByKey()
      .flatMap(t => {
        var result = List[LogRecord]()
        val iter = t._2.iterator
        var takeNext = true
        var (e1, t1, m1, o1) = ("", "", "", "")
        while (iter.hasNext) {
          if (takeNext) {
            val next = iter.next()
            e1 = next._1
            t1 = next._2
            m1 = next._3
            o1 = next._4
          }
          if (e1 == "pageload" && iter.hasNext) {
            val (e2, t2, m2, o2) = iter.next()
            if (e2 == "unload") {
              result = result ::: List(s"${t._1._1},${t._1._2},${t._1._3},${t2},${t._1._4},duration,,${t2.toLong - t1.toLong}".mkLogRecord)
              takeNext = true
            } else {
              e1 = e2
              t1 = t2
              m1 = m2
              o1 = o2
              takeNext = false
            }
          }
        }
        result
      })
    durationRdd.union(remainRdd)
  }
}
