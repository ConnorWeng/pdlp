package com.icbc.pdlp

import org.apache.spark.rdd.RDD

/**
  * Created by ConnorWeng on 2015/11/26.
  */
class DurationLogMachine extends LogMachine {
  override def process(material: RDD[String]): RDD[String] = {
    val remainRdd = material.filter(l => (!l.contains("pageload") && !l.contains("unload")))
    val durationRdd = material.filter(l => l.contains("pageload") || l.contains("unload"))
      .map(l => {
        val parts = l.split(",", 8)
        ((parts(0), parts(1), parts(2), parts(4)) -> (parts(5), parts(3), parts(6), parts(7)))
      })
      .groupByKey()
      .flatMap(t => {
        var result = List[String]()
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
              result = result ::: List(s"${t._1._1},${t._1._2},${t._1._3},${t2},${t._1._4},duration,,${t2.toLong - t1.toLong}")
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
