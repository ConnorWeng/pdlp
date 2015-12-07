package com.icbc.pdlp

import com.icbc.pdlp.LogRecord.String2LogRecord
import org.apache.spark.rdd.RDD
import org.scalatest.ShouldMatchers

/**
  * Created by ConnorWeng on 2015/12/7.
  */
class ClickLogMachineSpec extends FunSpecWithSparkContextFixture with ShouldMatchers {
  describe("process") {
    it("should extract srcElement ant put it back into other field") { fixture =>
      val rdd: RDD[LogRecord] = fixture.sc.parallelize(List(
        "http://82.200.46.140,d418500a-1596-af34-f4a1-74dd215508fb,GDGGAKGTFRIEBMHGGLBJCWHQJVGZETJKAJJSAFJK,1444272812234,/cmas/servlet/com.icbc.cte.cs.servlet.CSReqServlet,click,某菜单,{\"srcElement\":\"btn1\"}".mkLogRecord))
      val material = new ClickLogMachine().process(rdd)
      material.collect().toList should be(List(LogRecord("http://82.200.46.140","d418500a-1596-af34-f4a1-74dd215508fb","GDGGAKGTFRIEBMHGGLBJCWHQJVGZETJKAJJSAFJK","1444272812234","/cmas/servlet/com.icbc.cte.cs.servlet.CSReqServlet","click","某菜单","btn1","")))
    }
  }
}
