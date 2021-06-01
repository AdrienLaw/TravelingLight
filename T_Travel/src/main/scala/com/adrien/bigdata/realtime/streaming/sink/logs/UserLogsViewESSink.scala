package com.adrien.bigdata.realtime.streaming.sink.logs

import com.adrien.bigdata.realtime.constant.QRealTimeConstant
import com.adrien.bigdata.realtime.rdo.QRealtimeDO.UserLogPageViewData
import com.adrien.bigdata.realtime.util.JsonUtil
import com.adrien.bigdata.realtime.util.es.ES6ClientUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.slf4j.{Logger, LoggerFactory}

/**
 * 用户行为日志页面浏览明细数据输出ES
 */
class UserLogsViewESSink(indexName:String) extends RichSinkFunction[UserLogPageViewData]{
  //日志记录
  val logger :Logger = LoggerFactory.getLogger(this.getClass)
  //ES客户端连接对象
  var transportClient: PreBuiltTransportClient = _

  /**
   * 连接es集群
   * @param parameters
   */
  override def open(parameters: Configuration): Unit = {
    //flink与es网络通信参数设置(默认虚核)
    System.setProperty("es.set.netty.runtime.available.processors", "false")
    transportClient = ES6ClientUtil.buildTransportClient()
    super.open(parameters)
  }

  override def invoke(value: UserLogPageViewData, context: SinkFunction.Context[_]): Unit = {
    try {
      //参数校验
      val result: java.util.Map[String,Object] = JsonUtil.gObject2Map(value)
      val checkResult: String = checkData(result)
      if (StringUtils.isNoneBlank(checkResult)) {
        //日志记录
        logger.error("Travel.ESRecord.sink.checkData.err{}", checkResult)
        return
      }
      //请求id
      val sid = value.sid
      handleData(indexName,indexName,sid,result)
    } catch {
      case exception: Exception => logger.error(exception.getMessage)
    }
  }


  def handleData(idxName :String, idxTypeName :String, esID :String,
                 value: java.util.Map[String,Object]): Unit = {
    val indexRequest = new IndexRequest(idxName,idxTypeName,esID)
    val response = transportClient.prepareUpdate(idxName,idxTypeName,esID)
      .setRetryOnConflict(QRealTimeConstant.ES_RETRY_NUMBER)
      .setDoc(true)
      .setUpsert(indexRequest)
      .get()
    if (response.status() != RestStatus.CREATED && response.status() != RestStatus.OK) {
      System.err.println("calculate es session record error!map:" + new ObjectMapper().writeValueAsString(value))
      throw new Exception("run exception:status:" + response.status().name())
    }
  }

  /**
   * 资源关闭
   */
  override def close(): Unit = {
    if (this.transportClient != null) {
      this.transportClient.close()
    }
  }

  /**
   * 参数校验
   * @param value
   * @return
   */
  def checkData(value :java.util.Map[String,Object]): String ={
    var msg = ""
    if(null == value){
      msg = "kafka.value is empty"
    }
    //行为类型
    val action = value.get(QRealTimeConstant.KEY_ACTION)
    if(null == action){
      msg = "Travel.ESSink.action  is null"
    }

    //行为类型
    val eventType = value.get(QRealTimeConstant.KEY_EVENT_TYPE)
    if(null == eventType){
      msg = "Travel.ESSink.eventtype  is null"
    }

    //时间
    val ctNode = value.get(QRealTimeConstant.CT)
    if(null == ctNode){
      msg = "Travel.ESSink.ct is null"
    }
    msg
  }

}
