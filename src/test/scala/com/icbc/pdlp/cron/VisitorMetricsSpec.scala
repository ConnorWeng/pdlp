package com.icbc.pdlp.cron

import org.scalatest.{ShouldMatchers, FunSpec}

/**
  * Created by ConnorWeng on 2016/2/5.
  */
class VisitorMetricsSpec extends FunSpec with ShouldMatchers {
  describe("getAppIdViaUrl") {
    val appMap = Map(
      "http://83.24.113.34" -> 1,
      "http://122.18.141.38" -> 2
    )
    it("should return 1 when url is http://83.24.113.34") {
      VisitorMetrics.getAppIdViaUrl("http://83.24.113.34", appMap) should be(1)
    }
    it("should throw exception when url is http://NotExist.com") {
      an [NoSuchElementException] should be thrownBy VisitorMetrics.getAppIdViaUrl("http://NotExist.com", appMap)
    }
  }
}
