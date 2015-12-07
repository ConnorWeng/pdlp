package com.icbc.pdlp

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.fixture

/**
  * Created by ConnorWeng on 2015/12/7.
  */
class FunSpecWithSparkContextFixture extends fixture.FunSpec {

  case class FixtureParam(sc: SparkContext)

  override protected def withFixture(test: OneArgTest) = {
    val sc = new SparkContext(new SparkConf().setAppName("SparkContextFixture").setMaster("local"))
    try {
      withFixture(test.toNoArgTest(FixtureParam(sc)))
    } finally {
      sc.stop()
    }
  }
}
