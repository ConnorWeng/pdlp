package com.icbc.pdlp

import java.util.function.Consumer

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.scalatest.{FunSpec, ShouldMatchers}

/**
  * Created by ConnorWeng on 2016/2/3.
  */
class HBaseClientSpec extends FunSpec with ShouldMatchers {
  describe("HBaseClientSpec") {
    it("should print all data in test table") {
      val config = HBaseConfiguration.create()
      config.addResource(new Path(sys.env("HBASE_CONF_DIR"), "hbase-site.xml"))
      val con = ConnectionFactory.createConnection(config)
      val scanner = con.getTable(TableName.valueOf("test")).getScanner(new Scan())
      scanner.forEach(new Consumer[Result] {
        override def accept(t: Result): Unit = {
          println(t)
        }
      })
      scanner.close()
    }
  }
}
