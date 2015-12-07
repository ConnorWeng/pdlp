package com.icbc.pdlp

import com.icbc.pdlp.LogParser.String2LogRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.ShouldMatchers

/**
  * Created by ConnorWeng on 2015/12/1.
  */
class MenuLogMachineSpec extends FunSpecWithSparkContextFixture with ShouldMatchers {
  describe("process") {
    it("should set menu field on pageload record according to preceding click record") { fixture =>
      val rdd: RDD[LogRecord] = fixture.sc.parallelize(List(
        "http://82.200.46.140,d418500a-1596-af34-f4a1-74dd215508fb,GDGGAKGTFRIEBMHGGLBJCWHQJVGZETJKAJJSAFJK,1444272812234,/cmas/servlet/com.icbc.cte.cs.servlet.CSReqServlet,click,某菜单,{}".mkLogRecord,
        "http://82.200.46.140,98jud00a-1596-af34-f4a1-74dd21550iuj,KKKGAKGTFRIEBMHGGLBJCWHQJVGZETJKAJJSAJJJ,1444272812237,/cmas/servlet/com.icbc.cte.cs.servlet.CSReqServlet,pageload,,{}".mkLogRecord,
        "http://82.200.46.140,d418500a-1596-af34-f4a1-74dd215508fb,GDGGAKGTFRIEBMHGGLBJCWHQJVGZETJKAJJSAFJK,1444272812336,/cmas/servlet/com.icbc.cte.cs.servlet.CSReqServlet,pageload,,{}".mkLogRecord))
      val material = new MenuLogMachine().process(rdd)
      material.collect().toList should be(List(
        "http://82.200.46.140,d418500a-1596-af34-f4a1-74dd215508fb,GDGGAKGTFRIEBMHGGLBJCWHQJVGZETJKAJJSAFJK,1444272812234,/cmas/servlet/com.icbc.cte.cs.servlet.CSReqServlet,click,某菜单,{}".mkLogRecord,
        "http://82.200.46.140,98jud00a-1596-af34-f4a1-74dd21550iuj,KKKGAKGTFRIEBMHGGLBJCWHQJVGZETJKAJJSAJJJ,1444272812237,/cmas/servlet/com.icbc.cte.cs.servlet.CSReqServlet,pageload,,{}".mkLogRecord,
        "http://82.200.46.140,d418500a-1596-af34-f4a1-74dd215508fb,GDGGAKGTFRIEBMHGGLBJCWHQJVGZETJKAJJSAFJK,1444272812336,/cmas/servlet/com.icbc.cte.cs.servlet.CSReqServlet,pageload,某菜单,{}".mkLogRecord))
    }
  }
}
