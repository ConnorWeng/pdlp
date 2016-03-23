package com.icbc.pdlp.cron

import java.text.SimpleDateFormat

import com.icbc.pdlp.db.DBService
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ConnorWeng on 2016/2/5.
  */
trait VisitorMetrics {this: DBService =>

  def valueOf(columnName: String)(implicit result: Result): String = {
    Bytes.toString(result.getValue("pe".getBytes, columnName.getBytes))
  }

  def getDailyRddFromHBase(sc: SparkContext): RDD[(String, String, String, String, String, String, String)] = {
    val config = HBaseConfiguration.create()
    config.addResource(new Path(sys.env("HBASE_CONF_DIR"), "hbase-site.xml"))
    config.set(TableInputFormat.INPUT_TABLE, "page_event")

    // TODO: skip old records, implement incremental analysis
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val rdd = sc.newAPIHadoopRDD(config, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    rdd.map(_._2)
      .map(r => {
        implicit val result = r
        (valueOf("appid"), valueOf("mid"), valueOf("sid"), valueOf("page"), valueOf("timestamp"), valueOf("e"), valueOf("bv"))
      })
      .filter(_._6 == "pageload")
      .map(t => t.copy[String, String, String, String, String, String, String](_5 = dateFormat.format(t._5.toLong)))
      .cache()
  }

  def generatePageViewDailyReport(dailyRdd: RDD[(String, String, String, String, String, String, String)], appMap: Map[String, Int]) = {
    val pageViewRdd = dailyRdd.map(t => ((t._1, t._5), 1))
      .reduceByKey((a, b) => a + b)
    archiveMetrics(pageViewRdd, "pageview", appMap)
  }

  def generateUniqueVisitorDailyReport(dailyRdd: RDD[(String, String, String, String, String, String, String)], appMap: Map[String, Int]) = {
    val uniqueVisitorDailyRdd = dailyRdd.map(t => ((t._1, t._2, t._5), 1))
      .distinct()
      .map(t => ((t._1._1, t._1._3), 1))
    archiveMetrics(uniqueVisitorDailyRdd, "uniquevisitor", appMap)
  }

  def generateSessionDailyReport(dailyRdd: RDD[(String, String, String, String, String, String, String)], appMap: Map[String, Int]) = {
    val sessionDailyRdd = dailyRdd.map(t => ((t._1, t._3, t._5), 1))
      .distinct()
      .map(t => ((t._1._1, t._1._3), 1))
    archiveMetrics(sessionDailyRdd, "session", appMap)
  }

  def generateBrowserDailyReport(dailyRdd: RDD[(String, String, String, String, String, String, String)], appMap: Map[String, Int], sc: SparkContext) = {
    val browserDailyRdd = dailyRdd.map(t => {
      val browser = t._7 match {
        case bv if bv.contains("MSIE 6") => "IE6"
        case bv if bv.contains("MSIE 7") => "IE7"
        case bv if bv.contains("MSIE 8") => "IE8"
        case bv if bv.contains("MSIE 9") => "IE9"
        case bv if bv.contains("rv:11") => "IE11"
        case bv if bv.contains("Chrome") => "Chrome"
        case _ => "browser_unknown"
      }
      (t._1, t._2, t._5, browser)
    })
    .distinct()
    .groupBy(_._4)
    .collect()
    .foreach(t => {
      val browser = t._1
      val pairRdd = sc.parallelize(t._2.map(d => ((d._1, d._3), 1)).toSeq)
      archiveMetrics(pairRdd, browser, appMap)
    })
  }

  def generatePageData(dailyRdd: RDD[(String, String, String, String, String, String, String)], appMap: Map[String, Int]) = {
    val pageRdd = dailyRdd.map(t => {
      (t._1, t._4)
    }).distinct()
    .foreachPartition { p =>
      dao.withConnection { con =>
        val stmt = con.createStatement()
        p.toList.foreach { t =>
          val appId = appMap(t._1)
          val page = t._2
          val sql =
            s"""
               |insert into page(page_url, app_id)
               |select '${page}', ${appId}
               |from dual
               |where not exists (
               |select 1 from page where page_url = '${page}' and app_id = ${appId}
               |);
             """.stripMargin
          stmt.addBatch(sql)
        }
        stmt.executeBatch()
        stmt.close()
      }
    }
  }

  def archiveMetrics(pairRdd: RDD[((String, String), Int)], metricsName: String, appMap: Map[String, Int]) = {
    val rdd = pairRdd.reduceByKey((a, b) => a + b)
      .map(t => {
        val appId = appMap(t._1._1)
        val date = t._1._2
        val value = t._2
        (appId, date, value)
      })
    insertMetrics(rdd, metricsName)
  }

  def insertMetrics(metricsRdd: RDD[(Int, String, Int)], metricsName: String) = {
    metricsRdd.foreachPartition { p =>
      dao.withConnection { con =>
        val stmt = con.createStatement()
        p.toList.foreach { t =>
          val sql =
            s"""
              |insert into archive_numeric(
              |  name, app_id, date1, date2, period, ts_archived, value)
              |select
              |  '$metricsName',
              |  ${t._1},
              |  '${t._2}',
              |  '${t._2}',
              |  1,
              |  now(),
              |  ${t._3}
              |from dual
              |where not exists (
              |  select 1 from archive_numeric
              |  where name = '$metricsName'
              |  and app_id = ${t._1}
              |  and date1 = '${t._2}'
              |  and period = 1
              |);
            """.stripMargin
          stmt.addBatch(sql)
        }
        stmt.executeBatch()
        stmt.close()
      }
    }
  }
}

object VisitorMetricsMain extends Serializable with VisitorMetrics with DBService {

  val dao = new BaseDAO
  val appDAO = new AppDAO

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("pai-distributed-log-parser-VisitorMetrics").setMaster("local")
    val sc = new SparkContext(conf)

    val dailyRdd = getDailyRddFromHBase(sc)
    val appMap = appDAO.findAll.map(app => (app.appUrl -> app.appId)).toMap

    generatePageData(dailyRdd, appMap)
    generatePageViewDailyReport(dailyRdd, appMap)
    generateUniqueVisitorDailyReport(dailyRdd, appMap)
    generateSessionDailyReport(dailyRdd, appMap)
    generateBrowserDailyReport(dailyRdd, appMap, sc)
  }

}