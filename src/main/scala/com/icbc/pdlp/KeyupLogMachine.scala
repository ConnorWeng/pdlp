package com.icbc.pdlp

import org.apache.spark.rdd.RDD

/**
  * Created by ConnorWeng on 2015/11/25.
  */
class KeyupLogMachine extends LogMachine {
  override def process(material: RDD[String]): RDD[String] = {
    material
  }
}
