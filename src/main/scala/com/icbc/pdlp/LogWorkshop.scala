package com.icbc.pdlp

import org.apache.spark.rdd.RDD

/**
  * Created by ConnorWeng on 2015/11/27.
  */
class LogWorkshop(rawMaterial: RDD[String], machineList: List[LogMachine] = Nil) {
  private var material: RDD[String] = null

  def process(): LogWorkshop = {
    material match {
      case null => {
        material = rawMaterial
        for (machine <- machineList) {
          material = machine process material
        }
      }
    }
    this
  }

  def sendTo(dealer: LogDealer): LogWorkshop = {
    dealer accept material
    this
  }
}
