package com.icbc.pdlp.cron

import java.text.SimpleDateFormat

import com.cloudera.spark.hbase.HBaseContext
import com.icbc.pdlp.db.{AppDAO, BaseDAO}
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
object VisitorMetrics {

  def valueOf(columnName: String)(implicit result: Result): String = {
    Bytes.toString(result.getValue("pe".getBytes, columnName.getBytes))
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("pai-distributed-log-parser-VisitorMetrics").setMaster("local")
    val sc = new SparkContext(conf)

    val dailyRdd = getDailyRddFromHBase(sc)
    val appMap = AppDAO.findAll.map(app => (app.appUrl -> app.appId)).toMap

    generatePageViewDailyReport(dailyRdd, appMap)
    generateUniqueVisitorDailyReport(dailyRdd, appMap)
    generateSessionDailyReport(dailyRdd, appMap)

    //TODO: insert into hbase table visitor_daily, column family vd
  }

  def getDailyRddFromHBase(sc: SparkContext): RDD[(String, String, String, String, String, String)] = {
    val config = HBaseConfiguration.create()
    config.addResource(new Path(sys.env("HBASE_CONF_DIR"), "hbase-site.xml"))
    config.set(TableInputFormat.INPUT_TABLE, "page_event")

    // TODO: skip old records, implement incremental analysis
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val rdd = sc.newAPIHadoopRDD(config, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    rdd.map(_._2)
      .map(r => {
        implicit val result = r
        (valueOf("appid"), valueOf("mid"), valueOf("sid"), valueOf("page"), valueOf("timestamp"), valueOf("e"))
      })
      .filter(_._6 == "pageload")
      .map(t => t.copy[String, String, String, String, String, String](_5 = dateFormat.format(t._5.toLong)))
      .cache()
  }

  def generatePageViewDailyReport(dailyRdd: RDD[(String, String, String, String, String, String)], appMap: Map[String, Int]) = {
    val pageViewRdd = dailyRdd.map(t => ((t._1, t._5), 1))
      .reduceByKey((a, b) => a + b)
      .map(t => {
        val appId = getAppIdViaUrl(t._1._1, appMap)
        val date = t._1._2
        val value = t._2
        (appId, date, value)
      })
    insertMetrics(pageViewRdd, "pageview")
  }

  def generateUniqueVisitorDailyReport(dailyRdd: RDD[(String, String, String, String, String, String)], appMap: Map[String, Int]) = {
    val uniqueVisitorDailyRdd = dailyRdd.map(t => ((t._1, t._2, t._5), 1))
      .distinct().map(t => ((t._1._1, t._1._3), 1))
      .reduceByKey((a, b) => a + b)
      .map(t => {
        val appId = getAppIdViaUrl(t._1._1, appMap)
        val date = t._1._2
        val value = t._2
        (appId, date, value)
      })
    insertMetrics(uniqueVisitorDailyRdd, "uniquevisitor")
  }

  def generateSessionDailyReport(dailyRdd: RDD[(String, String, String, String, String, String)], appMap: Map[String, Int]) = {
    val sessionDailyRdd = dailyRdd.map(t => ((t._1, t._3, t._5), 1))
      .distinct().map(t => ((t._1._1, t._1._3), 1))
      .reduceByKey((a, b) => a + b)
      .map(t => {
        val appId = getAppIdViaUrl(t._1._1, appMap)
        val date = t._1._2
        val value = t._2
        (appId, date, value)
      })
    insertMetrics(sessionDailyRdd, "session")
  }

  def getAppIdViaUrl(appUrl: String, appMap: Map[String, Int]): Int = {
    return appMap(appUrl)
  }

  def insertMetrics(metricsRdd: RDD[(Int, String, Int)], name: String) = {
    metricsRdd.foreachPartition { p =>
      val dao = new BaseDAO
      dao.withConnection { con =>
        val stmt = con.createStatement()
        p.toList.foreach { t =>
          val sql =
            s"""
              |insert into archive_numeric(
              |  name, app_id, date1, date2, period, ts_archived, value)
              |select
              |  '$name',
              |  ${t._1},
              |  '${t._2}',
              |  '${t._2}',
              |  1,
              |  now(),
              |  ${t._3}
              |from dual
              |where not exists (
              |  select 1 from archive_numeric
              |  where name = '$name'
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
