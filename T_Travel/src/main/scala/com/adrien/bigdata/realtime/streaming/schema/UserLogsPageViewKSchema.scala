package com.adrien.bigdata.realtime.streaming.schema

import java.lang

import com.adrien.bigdata.realtime.rdo.QRealtimeDO.UserLogPageViewData
import com.adrien.bigdata.realtime.util.{CommonUtil, JsonUtil}
import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.{KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

/**
 * 行为日志页面浏览数据(明细)kafka序列化
 */
class UserLogsPageViewKSchema(topic: String) extends KafkaSerializationSchema[UserLogPageViewData]
                                             with KafkaDeserializationSchema[UserLogPageViewData]{
  /**
   * 序列化
   * @param element
   * @param timestamp
   * @return
   */
  override def serialize(element: UserLogPageViewData, timestamp: lang.Long):
                                                    ProducerRecord[Array[Byte], Array[Byte]] = {
    val sid = element.sid
    val userDevice = element.userDevice
    val targetID = element.targetId
    val tmp = sid + userDevice+ targetID
    val key = CommonUtil.getMD5AsHex(tmp.getBytes())
    val value = JsonUtil.gObject2Json(element)
    new ProducerRecord[Array[Byte], Array[Byte]](topic,key.getBytes(),value.getBytes())
  }

  /**
   * 反序列化
   * @param record
   * @return
   */
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): UserLogPageViewData = {
    val value = record.value()
    val gson: Gson = new Gson()
    val log: UserLogPageViewData = gson.fromJson(new String(value),classOf[UserLogPageViewData])
    log
  }


  override def isEndOfStream(nextElement: UserLogPageViewData): Boolean = {
    return false
  }


  override def getProducedType: TypeInformation[UserLogPageViewData] = {
    TypeInformation.of(classOf[UserLogPageViewData])
  }
}
