package com.icbc.pdlp.db

import com.icbc.pdlp.model.Machine

/**
  * Created by ConnorWeng on 2016/1/26.
  */
object MachineDAO extends BaseDAO {
  def findAll: List[Machine] = {
    var machines = collection.mutable.Buffer[Machine]()
    super.findAll("machine") { rs =>
      while (rs.next()) {
        machines += Machine(rs.getInt("machine_id"), rs.getString("machine_code"),
          rs.getString("machine_name"), rs.getString("browser"), rs.getString("resolution"),
          rs.getInt("first_day_id"), rs.getInt("last_day_id"))
      }
    }
    machines.toList
  }

  def save(machines: List[Machine]) = {
    withConnection { con =>
      val stmt = con.createStatement()
      machines.foreach { machine =>
        val sql =
          s"""
            |insert into machine(
            |  machine_code, machine_name, browser, resolution, first_day_id, last_day_id
            |) values (
            |  '${machine.machineCode}', '${machine.machineName}', '${machine.browser}',
            |  '${machine.resolution}', ${machine.first_day_id}, ${machine.last_day_id}
            |);
          """.stripMargin
        stmt.addBatch(sql)
      }
      stmt.executeBatch()
      stmt.close()
    }
  }
}
