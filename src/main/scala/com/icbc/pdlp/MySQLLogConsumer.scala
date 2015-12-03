package com.icbc.pdlp

import java.util.Properties

import org.apache.spark.sql.{SaveMode, DataFrame}

/**
  * Created by ConnorWeng on 2015/11/27.
  */
class MySQLLogConsumer extends LogConsumer() {
  override def consume(production: DataFrame, tableName: String): Unit = {
    val properties = new Properties()
    properties.put("user", sys.env("mysql_user"))
    properties.put("password", sys.env("mysql_pass"))
    production.write.mode(SaveMode.Append)
      .jdbc(sys.env("mysql_url"), tableName, properties)
  }
}
