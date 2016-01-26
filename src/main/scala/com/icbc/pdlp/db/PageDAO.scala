package com.icbc.pdlp.db

import com.icbc.pdlp.model.Page

/**
  * Created by ConnorWeng on 2016/1/26.
  */
object PageDAO extends BaseDAO {
  def findAll: List[Page] = {
    var pages = collection.mutable.Buffer[Page]()
    super.findAll("page") { rs =>
      while (rs.next()) {
        pages += Page(rs.getInt("page_id"), rs.getString("page_url"), rs.getInt("app_id"), rs.getInt("module_id"))
      }
    }
    pages.toList
  }

  def save(pages: List[Page]) = {
    withConnection { con =>
      val stmt = con.createStatement()
      pages.foreach { page =>
        val sql =
          s"""
            |insert into page(page_url, app_id, module_id)
            |values ('${page.pageUrl}', ${page.appId}, ${page.moduleId});
          """.stripMargin
        stmt.addBatch(sql)
      }
      stmt.executeBatch()
      stmt.close()
    }
  }
}
