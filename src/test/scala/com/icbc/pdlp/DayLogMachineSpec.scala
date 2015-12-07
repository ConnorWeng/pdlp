package com.icbc.pdlp

import com.icbc.pdlp.LogParser.String2LogRecord
import org.apache.spark.rdd.RDD
import org.scalatest.ShouldMatchers

/**
  * Created by ConnorWeng on 2015/12/2.
  */
class DayLogMachineSpec extends FunSpecWithSparkContextFixture with ShouldMatchers {
  describe("process") {
    it("should set date field") { fixture =>
      val rdd: RDD[LogRecord] = fixture.sc.parallelize(List(
        "http://82.200.46.140,d418500a-1596-af34-f4a1-74dd215508fb,GDGGAKGTFRIEBMHGGLBJCWHQJVGZETJKAJJSAFJK,1444272812234,/cmas/servlet/com.icbc.cte.cs.servlet.CSReqServlet,click,某菜单,{}".mkLogRecord))
      val material = new DayLogMachine().process(rdd)
      material.collect().toList should be(List(
        LogRecord("http://82.200.46.140","d418500a-1596-af34-f4a1-74dd215508fb","GDGGAKGTFRIEBMHGGLBJCWHQJVGZETJKAJJSAFJK","1444272812234","/cmas/servlet/com.icbc.cte.cs.servlet.CSReqServlet","click","某菜单","{}","20151008")))
    }
  }
}
