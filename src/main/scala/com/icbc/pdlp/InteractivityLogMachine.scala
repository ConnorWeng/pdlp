package com.icbc.pdlp

import org.apache.spark.rdd.RDD

/**
  * Created by Connor on 12/28/15.
  */
class InteractivityLogMachine extends LogMachine {
  override def process(material: RDD[LogRecord]): RDD[LogRecord] = {
    material.filter(record => record.event == "click" || record.event == "mousemove" || record.event == "keyup")
  }
}
