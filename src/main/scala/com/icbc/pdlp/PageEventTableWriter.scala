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
    val conf = new SparkConf().setAppName("pai-distributed-log-parser-PageEventTableWriter").setMaster("local")
    val sc = new SparkContext(conf)

    val dateFormat = new SimpleDateFormat("yyyyMMdd")

    // TODO: read from directory
    val recordsRdd = sc.textFile(sys.env("log_path"))
      .filter(_ != "")
      .flatMap(LogParser.parseJsonLine)
      .map(_.mkLogRecord)
      .map(r => r.copy(date = dateFormat.format(Long.parseLong(r.timestamp))))

    val config = HBaseConfiguration.create()
    config.addResource(new Path(sys.env("HBASE_CONF_DIR"), "hbase-site.xml"))
    val hbaseContext = new HBaseContext(sc, config)

    hbaseContext.bulkPut[LogRecord](recordsRdd, "page_event", recordToPut, true)
  }

  def recordToPut(record: LogRecord): Put = {
    val rowKey = makeRowKey(record)
    val put = new Put(rowKey.getBytes)
    put.addColumn("pe".getBytes, "appid".getBytes, record.appId.getBytes)
    put.addColumn("pe".getBytes, "mid".getBytes, record.mid.getBytes)
    put.addColumn("pe".getBytes, "timestamp".getBytes, record.timestamp.getBytes)
    val other = LogParser.parseOther(record.other)
    other.foreach { case (k, v) =>
      put.addColumn("pe".getBytes, k.getBytes, v.getBytes)
    }
    put
  }

  def makeRowKey(record: LogRecord): String = {
    md5(record.appId) + md5(record.mid) + md5(record.page) + record.timestamp
  }
}
