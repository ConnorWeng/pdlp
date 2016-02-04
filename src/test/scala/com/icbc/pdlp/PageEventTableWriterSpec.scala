package com.icbc.pdlp

import com.icbc.pdlp.LogRecord._
import org.scalatest.{FunSpec, ShouldMatchers}

/**
  * Created by Connor on 2/3/16.
  */
class PageEventTableWriterSpec extends FunSpec with ShouldMatchers {
  describe("makeRowKey") {
    it("should return expected hash value") {
      PageEventTableWriter
        .makeRowKey(
          "http://82.200.46.140,d418500a-1596-af34-f4a1-74dd215508fb,GDGGAKGTFRIEBMHGGLBJCWHQJVGZETJKAJJSAFJK,1444272812234,/cmas/servlet/com.icbc.cte.cs.servlet.CSReqServlet,click,某菜单,{}".mkLogRecord
        ) should be("A5820AFF18FF3363A73AE6156D71D274BD7E837778C911BF69111B27F22B4B7FBD0F0F7C5AAA42BA159086A165B2A5541444272812234")
    }
  }

  describe("recordToPut") {
    val record = "http://82.200.46.140,d418500a-1596-af34-f4a1-74dd215508fb,GDGGAKGTFRIEBMHGGLBJCWHQJVGZETJKAJJSAFJK,1444272812234,/cmas/servlet/com.icbc.cte.cs.servlet.CSReqServlet,keyup,,{\"e\":\"keyup\",\"keyCode\":18,\"ctrlKey\":false,\"altKey\":false,\"shiftKey\":false,\"srcElement\":\"BODY\"}".mkLogRecord
    it("should contains appid column") {
      val put = PageEventTableWriter.recordToPut(record)
      val cell = put.get("pe".getBytes, "appid".getBytes).get(0)
      val valueBytes = cell.getValueArray
      val valueOffset = cell.getValueOffset
      val value = new String(valueBytes.splitAt(valueOffset)._2)
      value should be("http://82.200.46.140")
    }
  }
}
