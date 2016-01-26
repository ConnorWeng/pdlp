package com.icbc.pdlp.model

/**
  * Created by ConnorWeng on 2016/1/26.
  */
case class PageEvent(pageEventId: Int, machineId: Int, sessionCode: String, timestamp: Long,
                     pageId: Int, eventType: String, eventData: String, dayId: Int) {}

