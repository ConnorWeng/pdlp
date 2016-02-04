package com.icbc.pdlp

import java.lang.Long
import java.text.SimpleDateFormat

import akka.util.Crypt.md5
import com.cloudera.spark.hbase.HBaseContext
import com.icbc.pdlp.LogRecord._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Connor on 2/3/16.
  */
object PageEventTableWriter {
  def main(args: Array[String]) = {
    // TODO: read file, parse file, write to HFile
    val conf = new SparkConf().setAppName("pai-distributed-log-parser-hfilewriter").setMaster("local")
    val sc = new SparkContext(conf)

    val dateFormat = new SimpleDateFormat("yyyyMMdd")

    val recordsRdd = sc.textFile(sys.env("log_path"))
      .filter(_ != "")
      .flatMap(LogParser.parseJsonLine)
      .map(_.mkLogRecord)
      .map(r => r.copy(date = dateFormat.format(Long.parseLong(r.timestamp))))

    val config = HBaseConfiguration.create()
    config.addResource(new Path(sys.env("HBASE_CONF_DIR"), "hbase-site.xml"))
    val hbaseContext = new HBaseContext(sc, config)

    hbaseContext.bulkPut[LogRecord](recordsRdd, "page_event", (record) => {
      val rowKey = makeRowKey(record)
      val put = new Put(rowKey.getBytes)
      put.addColumn("pe".getBytes, "appid".getBytes, record.appId.getBytes)
      put.addColumn("pe".getBytes, "mid".getBytes, record.mid.getBytes)
      put
    }, true)
  }

  def makeRowKey(record: LogRecord): String = {
    md5(record.appId) + md5(record.mid) + md5(record.page) + record.timestamp
  }
}
