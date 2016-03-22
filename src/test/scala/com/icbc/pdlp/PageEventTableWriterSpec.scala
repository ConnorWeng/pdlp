package com.icbc.pdlp

import com.icbc.pdlp.LogRecord._
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.Put
import org.scalatest.{FunSpec, ShouldMatchers}

/**
  * Created by Connor on 2/3/16.
  */
class PageEventTableWriterSpec extends FunSpec with ShouldMatchers {
  describe("parseJsonLine") {
    it("should return a list of events") {
      PageEventTableWriter.parseJsonLine(
        """
          |{
          |    "appid": "http:\/\/83.24.113.34",
          |    "mid": "6aefcb9e-f2a5-5d86-33ea-f7b1359c2437",
          |    "sessions": [
          |        {
          |            "sid": 1447029967035,
          |            "pa": [
          |                {
          |                    "t": 1447029967037,
          |                    "p": "\/cmas\/logon.jsp",
          |                    "v": {
          |                        "e": "pageload",
          |                        "viewport": [1676,929]
          |                    }
          |                },
          |                {
          |                    "t": 1447029967207,
          |                    "p": "\/cmas\/logon.jsp",
          |                    "v": {
          |                        "e": "mousemove"
          |                    }
          |                },
          |                {
          |                    "t": 1447029967389,
          |                    "p": "\/cmas\/logon.jsp",
          |                    "v": {
          |                        "e": "resize"
          |                    }
          |                }
          |            ]
          |        },
          |        {
          |            "sid": 1447029967059,
          |            "pa": [
          |                {
          |                    "t": 1447029967068,
          |                    "p": "\/cmas\/index2.jsp",
          |                    "v": {
          |                        "e": "pageload"
          |                    }
          |                },
          |                {
          |                    "t": 1447029967097,
          |                    "p": "\/cmas\/index2.jsp",
          |                    "v": {
          |                        "e": "mousemove",
          |                        "ctpmenu": "任意菜单"
          |                    }
          |                }
          |            ]
          |        }
          |    ]
          |}
        """.stripMargin) should be(List(
        "http://83.24.113.34,6aefcb9e-f2a5-5d86-33ea-f7b1359c2437,1447029967035,1447029967037,/cmas/logon.jsp,pageload,,{\"e\":\"pageload\",\"viewport\":[1676,929]}",
        "http://83.24.113.34,6aefcb9e-f2a5-5d86-33ea-f7b1359c2437,1447029967035,1447029967207,/cmas/logon.jsp,mousemove,,{\"e\":\"mousemove\"}",
        "http://83.24.113.34,6aefcb9e-f2a5-5d86-33ea-f7b1359c2437,1447029967035,1447029967389,/cmas/logon.jsp,resize,,{\"e\":\"resize\"}",
        "http://83.24.113.34,6aefcb9e-f2a5-5d86-33ea-f7b1359c2437,1447029967059,1447029967068,/cmas/index2.jsp,pageload,,{\"e\":\"pageload\"}",
        "http://83.24.113.34,6aefcb9e-f2a5-5d86-33ea-f7b1359c2437,1447029967059,1447029967097,/cmas/index2.jsp,mousemove,任意菜单,{\"e\":\"mousemove\",\"ctpmenu\":\"任意菜单\"}"))
    }
  }

  describe("parseOther") {
    it("should return list of tuple2") {
      PageEventTableWriter.parseOther(
        "{\"e\":\"keyup\",\"keyCode\":18,\"ctrlKey\":false}"
      ) should be(List(("e", "keyup"), ("keycode", "18"), ("ctrlkey", "false")))
    }
    it("should return list of tuple2 with sc value untouched") {
      PageEventTableWriter.parseOther(
        "{\"sc\":{\"x\":1402,\"y\":890,\"xp\":11454,\"yp\":10974}}"
      ) should be(List(("sc", "{\"x\":1402,\"y\":890,\"xp\":11454,\"yp\":10974}")))
    }
  }

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
