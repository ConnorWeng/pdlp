package com.icbc.pdlp

import com.icbc.pdlp.LogRecord.String2LogRecord
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._
import org.json4s.native.JsonParser

/**
  * Created by ConnorWeng on 2015/11/12.
  */
object LogParser {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("pai-distributed-log-parser").setMaster("local")
    val sc = new SparkContext(conf)

    val rawMaterial = sc.textFile(sys.env("log_path"))
      .filter(_ != "")
      .flatMap(parseJsonLine)
      .map(_.mkLogRecord)

    new LogWorkshop(rawMaterial, List(new MenuLogMachine, new DurationLogMachine, new DayLogMachine, new ClickLogMachine))
      .process()
      .sendTo(new MenuLogDealer(new MySQLLogConsumer, sc))

    new LogWorkshop(rawMaterial, List(new InteractivityLogMachine))
      .process()
      .sendTo(new InteractivityLogDealer(new MySQLLogConsumer, sc))

    sc.stop()
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
}
