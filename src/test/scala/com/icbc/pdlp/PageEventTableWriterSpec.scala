package com.icbc.pdlp

import com.icbc.pdlp.LogRecord._
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.Put
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
    val record = "http://82.200.46.140,d418500a-1596-af34-f4a1-74dd215508fb,GDGGAKGTFRIEBMHGGLBJCWHQJVGZETJKAJJSAFJK,1444272812234,/cmas/servlet/com.icbc.cte.cs.servlet.CSReqServlet,keyup,,{\"e\":\"keyup\",\"x\":15,\"keyCode\":18,\"ctrlKey\":false,\"altKey\":false,\"shiftKey\":false,\"srcElement\":\"BODY\"}".mkLogRecord
    val put = PageEventTableWriter.recordToPut(record)

    it("should contains columns exactly") {
      valueOf(cellOf(put, "appid")) should be("http://82.200.46.140")
      valueOf(cellOf(put, "mid")) should be("d418500a-1596-af34-f4a1-74dd215508fb")
      valueOf(cellOf(put, "sid")) should be("GDGGAKGTFRIEBMHGGLBJCWHQJVGZETJKAJJSAFJK")
      valueOf(cellOf(put, "page")) should be("/cmas/servlet/com.icbc.cte.cs.servlet.CSReqServlet")
      valueOf(cellOf(put, "timestamp")) should be("1444272812234")
      valueOf(cellOf(put, "other")) should be("{\"e\":\"keyup\",\"x\":15,\"keyCode\":18,\"ctrlKey\":false,\"altKey\":false,\"shiftKey\":false,\"srcElement\":\"BODY\"}")
      valueOf(cellOf(put, "e")) should be("keyup")
      valueOf(cellOf(put, "keycode")) should be("18")
      valueOf(cellOf(put, "x")) should be("15")
    }

    def cellOf(put: Put, columnName: String) = {
      put.get("pe".getBytes, columnName.getBytes).get(0)
    }

    def valueOf(cell: Cell): String = {
      val valueBytes = cell.getValueArray
      val valueOffset = cell.getValueOffset
      val value = new String(valueBytes.splitAt(valueOffset)._2)
      value
    }
  }
}
