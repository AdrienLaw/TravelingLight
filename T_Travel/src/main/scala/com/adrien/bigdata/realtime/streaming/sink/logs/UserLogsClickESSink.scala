package com.adrien.bigdata.realtime.streaming.sink.logs

import com.adrien.bigdata.realtime.constant.QRealTimeConstant
import com.adrien.bigdata.realtime.rdo.QRealtimeDO.UserLogClickData
import com.adrien.bigdata.realtime.util.JsonUtil
import com.adrien.bigdata.realtime.util.es.ES6ClientUtil
import org.apache.commons.lang3.StringUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.{Logger, LoggerFactory}

class UserLogsClickESSink(index: String) extends RichSinkFunction[UserLogClickData]{
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
    System.setProperty("es.set.netty.runtime.available.processors","false")
    transportClient = ES6ClientUtil.buildTransportClient()
    super.open(parameters)
  }

  /**
   * Sink输出处理
   * @param value
   * @param context
   */
  override def invoke(value: UserLogClickData, context: SinkFunction.Context[_]): Unit = {
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

    }

  }

  def handleData(idxName: String,idxTypeName: String,esID: String,
                 value: Map[String,Object]): Unit = {
    val indexRequest = new IndexRequest(idxName,idxTypeName,esID)
  }


  def checkData (value: java.util.Map[String,Object]): String= {
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
