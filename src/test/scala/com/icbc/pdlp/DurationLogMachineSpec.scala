package com.icbc.pdlp

import com.icbc.pdlp.LogParser.String2LogRecord
import org.apache.spark.rdd.RDD
import org.scalatest.ShouldMatchers

/**
  * Created by ConnorWeng on 2015/11/27.
  */
class DurationLogMachineSpec extends FunSpecWithSparkContextFixture with ShouldMatchers {
  describe("process") {
    it("should compute duration from pageload and unload events") { fixture =>
      val rdd: RDD[LogRecord] = fixture.sc.parallelize(List(
        "http://82.200.46.140,d418500a-1596-af34-f4a1-74dd215508fb,GDGGAKGTFRIEBMHGGLBJCWHQJVGZETJKAJJSAFJK,1444270627113,/cmas/signin.flowc,pageload,某菜单,{\"e\":\"pageload\",\"viewport\":[1676,929],\"t\":939,\"pos\":[0,55],\"b\":\"Microsoft Internet Explorer、 screen -> List(1680、 1010)、 bv -> Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Tablet PC 2.0; .NET CLR 1.1.4322; .NET4.0C; .NET4.0E\"}".mkLogRecord,
        "http://82.200.46.140,d418500a-1596-af34-f4a1-74dd215508fb,GDGGAKGTFRIEBMHGGLBJCWHQJVGZETJKAJJSAFJK,1444272812234,/cmas/signin.flowc,unload,,{\"e\":\"unload\"}".mkLogRecord,
        "http://82.200.46.140,d418500a-1596-af34-f4a1-74dd215508fb,GDGGAKGTFRIEBMHGGLBJCWHQJVGZETJKAJJSAFJK,1444272812256,/cmas/signin.flowc,click,,{\"e\":\"click\"}".mkLogRecord))
      val material = new DurationLogMachine().process(rdd)
      material.collect().toList should be(List(
        "http://82.200.46.140,d418500a-1596-af34-f4a1-74dd215508fb,GDGGAKGTFRIEBMHGGLBJCWHQJVGZETJKAJJSAFJK,1444272812234,/cmas/signin.flowc,duration,某菜单,2185121".mkLogRecord,
        "http://82.200.46.140,d418500a-1596-af34-f4a1-74dd215508fb,GDGGAKGTFRIEBMHGGLBJCWHQJVGZETJKAJJSAFJK,1444272812256,/cmas/signin.flowc,click,,{\"e\":\"click\"}".mkLogRecord))
    }
  }
}
