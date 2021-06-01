package com.adrien.bigdata.realtime.streaming.schema

import java.lang

import com.adrien.bigdata.realtime.rdo.QRealtimeDO.UserLogData
import com.adrien.bigdata.realtime.util.JsonUtil
import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.{KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

/**
 * 行为日志kafka序列化
 * @param topic
 */
class UserLogsKSchema(topic: String) extends KafkaSerializationSchema[UserLogData] with KafkaDeserializationSchema[UserLogData]{
  /**
   * 序列化
   * @param element
   * @param timestamp
   * @return
   */
  override def serialize(element: UserLogData, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val key = element.sid
    val value =JsonUtil.gObject2Json(element)
    new ProducerRecord[Array[Byte], Array[Byte]](topic,key.getBytes,value.getBytes())
  }

  /**
   * 反序列化
   * @param record
   * @return
   */
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): UserLogData = {
    val value = record.value()
    val gson: Gson = new Gson()
    val log :UserLogData = gson.fromJson(new String(value),classOf[UserLogData])
    log
  }

  override def isEndOfStream(t: UserLogData): Boolean = {
    false
  }

  override def getProducedType: TypeInformation[UserLogData] = {
    TypeInformation.of(classOf[UserLogData])
  }
}
