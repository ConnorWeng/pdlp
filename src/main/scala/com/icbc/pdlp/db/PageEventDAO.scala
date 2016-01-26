package com.icbc.pdlp.db

import com.icbc.pdlp.model.PageEvent

/**
  * Created by ConnorWeng on 2016/1/25.
  */
object PageEventDAO extends BaseDAO {
  def save(pageEvents: List[PageEvent]) = {
    withConnection { con =>
      val stmt = con.createStatement()
      pageEvents.foreach { pageEvent =>
        val sql =
          s"""
             |insert into page_event(
             |  machine_id, session_code, timestamp, page_id, event_type, event_data, day_id
             |) values (
             |  ${pageEvent.machineId}, '${pageEvent.sessionCode}', ${pageEvent.timestamp},
             |  ${pageEvent.pageId}, '${pageEvent.eventType}', '${pageEvent.eventData}',
             |  ${pageEvent.dayId}
             |);
           """.stripMargin
        stmt.addBatch(sql)
      }
      stmt.executeBatch()
      stmt.close()
    }
  }
}
