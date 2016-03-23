package com.icbc.pdlp.db

import java.sql.{Connection, DriverManager, ResultSet}

import com.icbc.pdlp.model.App

import scala.util.{Failure, Try}

/**
  * Created by ConnorWeng on 2016/1/26.
  */
trait DBService {

  val dao: BaseDAO
  val appDAO: AppDAO

  class BaseDAO extends Serializable {
    val dbUrl = s"${sys.env("MYSQL_URL")}?user=${sys.env("MYSQL_USER")}&password=${sys.env("MYSQL_PASS")}"

    def withConnection(functionNeedConnection: (Connection) => Unit) = {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      Try({
        val con = DriverManager.getConnection(dbUrl)
        functionNeedConnection(con)
        con.close()
      }) match {
        case Failure(error) => {
          println(error.getMessage)
        }
        case _ => Nil
      }
    }

    def findAll(table: String, where: String = "")(functionNeedResultSet: (ResultSet) => Unit) = {
      withConnection { con =>
        val stmt = con.createStatement()
        val sql = s"select * from $table $where"
        val rs = stmt.executeQuery(sql)
        functionNeedResultSet(rs)
        rs.close()
      }
    }
  }

  class AppDAO extends BaseDAO {
    def findAll: List[App] = {
      var apps = collection.mutable.Buffer[App]()
      super.findAll("app") { rs =>
        while (rs.next()) {
          apps += App(rs.getInt("app_id"), rs.getString("app_name"),
            rs.getString("app_url"), rs.getInt("department_id"))
        }
      }
      apps.toList
    }
  }
}