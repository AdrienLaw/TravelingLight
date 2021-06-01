package com.adrien.bigdata.realtime.streaming.funs.orders

import com.adrien.bigdata.realtime.constant.QRealTimeConstant
import com.adrien.bigdata.realtime.rdo.QRealtimeDO.OrderDetailData
import com.adrien.bigdata.realtime.util.JsonUtil
import org.apache.flink.api.common.functions.MapFunction

/**
 * 旅游订单业务ETL相关函数
 */
object OrdersETLFun {
  class OrderDetailDataMapFun extends MapFunction[String,OrderDetailData]{
    override def map(value: String): OrderDetailData = {
      //根据行为和事件数据进行扩展信息提取
      val record :java.util.Map[String,String] = JsonUtil.json2object(value, classOf[java.util.Map[String,String]])
      //订单ID
      val orderID :String = record.getOrDefault(QRealTimeConstant.KEY_ORDER_ID,"")
      //用户ID
      val userID :String = record.getOrDefault(QRealTimeConstant.KEY_USER_ID,"")
      //产品ID
      val productID :String = record.getOrDefault(QRealTimeConstant.KEY_PRODUCT_ID,"")
      //酒店ID
      val pubID :String = record.getOrDefault(QRealTimeConstant.KEY_PUB_ID,"")
      //用户手机
      val userMobile :String = record.getOrDefault(QRealTimeConstant.KEY_USER_MOBILE,"")
      //用户所在地区
      val userRegion :String = record.getOrDefault(QRealTimeConstant.KEY_USER_REGION,"")
      //交通出行
      val traffic :String = record.getOrDefault(QRealTimeConstant.KEY_TRAFFIC,"")
      //交通出行等级
      val trafficGrade :String = record.getOrDefault(QRealTimeConstant.KEY_TRAFFIC_GRADE,"")
      //交通出行类型
      val trafficType :String = record.getOrDefault(QRealTimeConstant.KEY_TRAFFIC_TYPE,"")
      //交通出行类型
      val price :Int = record.getOrDefault(QRealTimeConstant.KEY_PRODUCT_PRICE,"").toInt
      //交通出行类型
      val fee :Int = record.getOrDefault(QRealTimeConstant.KEY_PRODUCT_FEE,"").toInt
      //折扣
      val hasActivity :String = record.getOrDefault(QRealTimeConstant.KEY_HAS_ACTIVITY,"")
      //成人、儿童、婴儿
      val adult :String = record.getOrDefault(QRealTimeConstant.KEY_PRODUCT_ADULT,"")
      val yonger :String = record.getOrDefault(QRealTimeConstant.KEY_PRODUCT_YONGER,"")
      val baby :String = record.getOrDefault(QRealTimeConstant.KEY_PRODUCT_BABY,"")
      //事件时间
      val ct :Long = record.getOrDefault(QRealTimeConstant.KEY_ORDER_CT,"").toLong
      OrderDetailData(orderID, userID, productID, pubID,
        userMobile, userRegion, traffic, trafficGrade, trafficType,
        price, fee, hasActivity,
        adult, yonger, baby, ct)
    }
  }
}
