package com.icbc.pdlp

import java.text.SimpleDateFormat

import com.icbc.pdlp.LogRecord.String2LogRecord
import com.icbc.pdlp.db._
import com.icbc.pdlp.model.{Machine, Module, Page, PageEvent}
import org.apache.spark.rdd.RDD
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

    // FIXME: this line of code may be not work correctly
    val material = new MenuLogMachine().process(rawMaterial)

    val appUrls = material.map(_.appId).collect().distinct.toList
    val appMap = makeAppMap(appUrls)

    val moduleNames = material.map(_.menu).filter(_ != "").collect().distinct.toList
    val moduleMap = makeModuleMap(moduleNames, material, appMap)

    val machineCodes = material.map(_.mid).collect().distinct.toList
    val machineMap = makeMachineMap(machineCodes)

    val pageUrls = material.map(_.page).collect().distinct.toList
    val pageMap = makePageMap(pageUrls, material, appMap, moduleMap)

    material.foreachPartition { ms =>
      val dateFormat = new SimpleDateFormat("yyyyMMdd")
      PageEventDAO.save(ms.toList.map { r =>
        // TODO: use DayLogMachine
        val date = dateFormat.format(r.timestamp.toLong).toInt
        PageEvent(0, machineMap.getOrElse(r.mid, 0), r.sid, r.timestamp.toLong, pageMap.getOrElse(r.page, 0), r.event, r.other, date)
      })
    }

    sc.stop()
  }

  def makeAppMap(appUrls: List[String]): Map[String, Int] = {
    AppDAO.findAll
      .filter(app => appUrls.contains(app.appUrl))
      .map(app => (app.appUrl -> app.appId))
      .toMap
  }

  def makeMachineMap(machineCodes: List[String]): Map[String, Int] = {
    val existMachines = MachineDAO.findAll
    val existMachineCodes = existMachines.map(_.machineCode)
    val nonexistMachineCodes = machineCodes.filter(!existMachineCodes.contains(_))
    MachineDAO.save(nonexistMachineCodes.map { machineCode =>
      Machine(0, machineCode, machineCode, "", "", 0, 0)
    })
    MachineDAO.findAll.map(machine => (machine.machineCode -> machine.machineId)).toMap
  }

  def makeModuleMap(moduleNames: List[String], material: RDD[LogRecord], appMap: Map[String, Int]): Map[String, Int] = {
    val existModules = ModuleDAO.findAll
    val existModuleNames = existModules.map(_.moduleName)
    val nonexistModuleNames = moduleNames.filter(!existModuleNames.contains(_))
    ModuleDAO.save(material.filter(m => nonexistModuleNames.contains(m.menu)).map(m =>
      Module(0, m.menu, "", appMap(m.appId))
    ).collect().distinct.toList)
    ModuleDAO.findAll.map(module => (module.moduleName -> module.moduleId)).toMap
  }

  def makePageMap(pageUrls: List[String], material: RDD[LogRecord], appMap: Map[String, Int], moduleMap: Map[String, Int]): Map[String, Int] = {
    val existPages = PageDAO.findAll
    val existPageUrls = existPages.map(_.pageUrl)
    val nonexistPageUrls = pageUrls.filter(!existPageUrls.contains(_))
    // FIXME: must have duplicated records because of module_id
    PageDAO.save(material.filter(m => nonexistPageUrls.contains(m.page)).map(m =>
      Page(0, m.page, appMap.getOrElse(m.appId, 0), moduleMap.getOrElse(m.menu, 0))
    ).collect().distinct.toList)
    PageDAO.findAll.map(page => (page.pageUrl -> page.pageId)).toMap
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
}
