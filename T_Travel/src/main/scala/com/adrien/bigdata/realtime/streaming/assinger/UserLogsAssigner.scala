package com.adrien.bigdata.realtime.streaming.assinger

import com.adrien.bigdata.realtime.rdo.QRealtimeDO.UserLogData
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
 * 用户行为日志事件时间辅助器
 * @param maxOutOfOrderness
 */
class UserLogsAssigner(maxOutOfOrderness: Long) extends AssignerWithPeriodicWatermarks[UserLogData]{
  //当前时间戳
  var currentMaxTimestamp :Long = java.lang.Long.MIN_VALUE

  /**
   * 水印生成
   * @return
   */
  override def getCurrentWatermark: Watermark = {
    var watermark: Long = java.lang.Long.MIN_VALUE
    if (currentMaxTimestamp != java.lang.Long.MIN_VALUE) {
      watermark = currentMaxTimestamp - watermark
    }
    new Watermark(watermark)
  }

  /**
   * 事件时间提取
   * @param element
   * @param previousElementTimestamp
   * @return
   */
  override def extractTimestamp(element: UserLogData, previousElementTimestamp: Long): Long = {
    val ct = element.ct
    currentMaxTimestamp = Math.max(ct,currentMaxTimestamp)
    ct
  }
}
