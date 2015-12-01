package com.icbc.pdlp

import org.scalatest.{FunSpec, ShouldMatchers}

/**
  * Created by ConnorWeng on 2015/11/12.
  */
class LogParserSpec extends FunSpec with ShouldMatchers {
  describe("parseJsonLine") {
    it("should return a list of events") {
      LogParser.parseJsonLine(
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
}
