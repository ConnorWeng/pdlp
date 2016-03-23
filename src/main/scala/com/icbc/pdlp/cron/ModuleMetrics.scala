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
  * Created by Connor on 2/15/16.
  */
trait ModuleMetrics {this: DBService =>

  def getDailyRddFromHBase(sc: SparkContext, appMap: Map[String,Int]): RDD[((Int, String, String, String), Int)] = {
    val config = HBaseConfiguration.create()
    config.addResource(new Path(sys.env("HBASE_CONF_DIR"), "hbase-site.xml"))
    config.set(TableInputFormat.INPUT_TABLE, "page_event")

    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val rdd = sc.newAPIHadoopRDD(config, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    rdd.map(_._2)
      .map(r => {
        implicit val result = r
        (valueOf("appid"), valueOf("timestamp"), valueOf("ctpmenu"), valueOf("mid"))
      })
      .filter(_._3 != null)
      .map(t => ((appMap(t._1), dateFormat.format(t._2.toLong), t._3, t._4), 1))
      .cache()
  }

  def valueOf(columnName: String)(implicit result: Result): String = {
    Bytes.toString(result.getValue("pe".getBytes, columnName.getBytes))
  }

  def insertBlob(blobRdd: RDD[(Int, String, String)], blobName: String) = {
    blobRdd.foreachPartition { p =>
      dao.withConnection { con =>
        val stmt = con.createStatement()
        p.toList.foreach { t =>
          val sql =
            s"""
               |insert into archive_blob(
               |  name, app_id, date1, date2, period, ts_archived, value)
               |select
               |  '$blobName',
               |  ${t._1},
               |  '${t._2}',
               |  '${t._2}',
               |  1,
               |  now(),
               |  '${t._3}'
               |from dual
               |where not exists (
               |  select 1 from archive_numeric
               |  where name = '$blobName'
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

object ModuleMetricsMain extends Serializable with ModuleMetrics with DBService {

  val dao = new BaseDAO
  val appDAO = new AppDAO

  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("pai-distributed-log-parser-ModuleMetrics").setMaster("local")
    val sc = new SparkContext(conf)

    val appMap = appDAO.findAll.map(app => (app.appUrl -> app.appId)).toMap
    val moduleDailyRdd = getDailyRddFromHBase(sc, appMap)
      .reduceByKey((a, b) => a + b)
      .map(t => {
        val appId = t._1._1
        val date = t._1._2
        val ctpmenu = t._1._3
        val mid = t._1._4
        val count = t._2
        ((appId, date), (ctpmenu, count, mid))
      })
      .groupByKey()
      .map(t => {
        val appId = t._1._1
        val date = t._1._2
        val list = t._2.toList
        val menuList = list
          .groupBy(_._1)
          .map(t => {
            (t._1, t._2.map(_._2).sum, t._2.map(_._3).mkString(","))
          })
        var blob = "{"
        menuList.foreach(bt => blob += s""""${bt._1}":[${bt._2},"${bt._3}"],""")
        blob = blob.substring(0, blob.size - 1) + "}"
        (appId, date, blob)
      })
    insertBlob(moduleDailyRdd, "module")
  }

}
