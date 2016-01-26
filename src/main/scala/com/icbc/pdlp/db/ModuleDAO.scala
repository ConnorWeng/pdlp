package com.icbc.pdlp.db

import com.icbc.pdlp.model.Module

/**
  * Created by ConnorWeng on 2016/1/26.
  */
object ModuleDAO extends BaseDAO {
  def findAll: List[Module] = {
    var modules = collection.mutable.Buffer[Module]()
    super.findAll("module") { rs =>
      while (rs.next()) {
        modules += Module(rs.getInt("module_id"), rs.getString("module_name"), rs.getString("module_url"), rs.getInt("app_id"))
      }
    }
    modules.toList
  }

  def save(modules: List[Module]) = {
    withConnection { con =>
      val stmt = con.createStatement()
      modules.foreach { module =>
        val sql =
          s"""
            |insert into module(module_name, module_url, app_id)
            |values (
            |  '${module.moduleName}', '${module.moduleUrl}', ${module.appId}
            |);
          """.stripMargin
        stmt.addBatch(sql)
      }
      stmt.executeBatch()
      stmt.close()
    }
  }
}
