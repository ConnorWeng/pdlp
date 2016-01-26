package com.icbc.pdlp

import org.apache.spark.rdd.RDD

/**
  * Created by ConnorWeng on 2015/11/27.
  */
class LogWorkshop(rawMaterial: RDD[LogRecord], machineList: List[LogMachine] = Nil) {
  var material: RDD[LogRecord] = null

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
}
