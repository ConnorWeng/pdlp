package com.icbc.pdlp.db

import com.icbc.pdlp.model.App

/**
  * Created by ConnorWeng on 2016/1/26.
  */
object AppDAO extends BaseDAO {
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
