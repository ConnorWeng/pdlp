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
import org.json4s.JsonAST.{JNull, JArray}
import org.json4s.jackson.JsonMethods._
import org.json4s.native.JsonParser
import org.json4s.{JObject, JValue}

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
      .flatMap(parseJsonLine)
      .map(_.mkLogRecord)
      .map(r => r.copy(date = dateFormat.format(Long.parseLong(r.timestamp))))

    val config = HBaseConfiguration.create()
    config.addResource(new Path(sys.env("HBASE_CONF_DIR"), "hbase-site.xml"))
    val hbaseContext = new HBaseContext(sc, config)

    hbaseContext.bulkPut[LogRecord](recordsRdd, "page_event", recordToPut, true)
  }

  def parseJsonLine(line: String): List[String] = {
    var result = List[String]()
    val lineValue = JsonParser.parse(line)
    val sessionList: List[(JValue, JValue, JValue, JValue)] = for {
      JArray(sessions) <- lineValue \\ "sessions"
      session <- sessions
      if !(session \ "sid").toOption.isEmpty
      JArray(pas) <- session \ "pa"
      if pas.toList(0).isInstanceOf[JObject]
      pa <- pas
    } yield (lineValue \ "appid", lineValue \ "mid", session \ "sid", pa)
    sessionList.foreach(x => {
      val appid = x._1.values
      val mid = x._2.values
      val sid = x._3.values
      val eValue = x._4.asInstanceOf[JValue]
      val e = (eValue \\ "e").values
      val t = (eValue \ "t").values
      val p = (eValue \ "p").values
      val ctpMenu = if ((eValue \\ "ctpmenu").values.isInstanceOf[String]) (eValue \\ "ctpmenu").values else ""
      val other = eValue \ "v"
      val o = compact(render(other))
      result = result ::: List(s"$appid,$mid,$sid,$t,$p,$e,$ctpMenu,$o")
    })
    result
  }

  def parseOther(other: String): List[(String, String)] = {
    val otherValue = JsonParser.parse(other)
    otherValue.asInstanceOf[JObject].obj.map { field =>
      if (field._2.values.isInstanceOf[Map[Any, Any]]) {
        (field._1.toLowerCase, compact(render(field._2)))
      } else {
        if (field._2 != JNull) {
          (field._1.toLowerCase, field._2.values.toString)
        } else {
          (field._1.toLowerCase, "")
        }
      }
    }
  }

  def recordToPut(record: LogRecord): Put = {
    val rowKey = makeRowKey(record)
    val put = new Put(rowKey.getBytes)
    put.addColumn("pe".getBytes, "appid".getBytes, record.appId.getBytes)
    put.addColumn("pe".getBytes, "mid".getBytes, record.mid.getBytes)
    put.addColumn("pe".getBytes, "sid".getBytes, record.sid.getBytes)
    put.addColumn("pe".getBytes, "page".getBytes, record.page.getBytes)
    put.addColumn("pe".getBytes, "timestamp".getBytes, record.timestamp.getBytes)
    put.addColumn("pe".getBytes, "other".getBytes, record.other.getBytes)
    val other = parseOther(record.other)
    other.foreach { case (k, v) =>
      put.addColumn("pe".getBytes, k.getBytes, v.getBytes)
    }
    put
  }

  def makeRowKey(record: LogRecord): String = {
    md5(record.appId) + md5(record.mid) + md5(record.page) + record.timestamp
  }
}
