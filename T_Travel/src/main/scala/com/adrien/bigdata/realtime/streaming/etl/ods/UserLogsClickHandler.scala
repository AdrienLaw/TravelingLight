package com.adrien.bigdata.realtime.streaming.etl.ods

import com.adrien.bigdata.realtime.constant.QRealTimeConstant
import com.adrien.bigdata.realtime.enumes.{ActionEnum, EventEnum}
import com.adrien.bigdata.realtime.rdo.QRealtimeDO.{UserLogClickData, UserLogData}
import com.adrien.bigdata.realtime.streaming.assinger.UserLogsAssigner
import com.adrien.bigdata.realtime.streaming.funs.logs.UserLogClickDataMapFun
import com.adrien.bigdata.realtime.streaming.schema.UserLogsKSchema
import com.adrien.bigdata.realtime.streaming.sink.logs.UserLogsClickESSink
import com.adrien.bigdata.realtime.util.help.FlinkHelper
import org.apache.flink.shaded.curator.org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.config.StartupMode
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema, KafkaSerializationSchema}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._


/**
 * 将用户行为日志数据---过滤出交互行为点击明细数据---打入ES
 */
object UserLogsClickHandler {
  //日志记录
  val logger: Logger = LoggerFactory.getLogger("UserLogsClickHandler")

  def handleLogsETL4ESJob(appName: String, groupId: String, fromTopic: String, indexName: String): Unit = {
    try {
      /**
       * 1 Flink环境初始化
       */
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment()
      /**
       * 2 kafka流式数据源
       *   kafka消费配置参数
       *   kafka消费策略
       *   创建flink消费对象FlinkKafkaConsumer
       *   用户行为日志(kafka数据)反序列化处理
       */
      val schema: KafkaDeserializationSchema[UserLogData] = new UserLogsKSchema(fromTopic)
      val kafkaConsumer: FlinkKafkaConsumer[UserLogData] = FlinkHelper.createKafkaSerDeConsumer(env,fromTopic,groupId,schema,StartupMode.LATEST)
      /**
       * 3 创建点击日志数据流
       *   (1)设置事件时间提取器及水位计算(如果是事件时间)
       *     方式：自定义实现AssignerWithPeriodicWatermarks 如 UserLogsAssigner
       *   (2) 数据过滤
       *   (3) 数据转换
       */
      val userLogsAssigner = new UserLogsAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
      val clickDStream :DataStream[UserLogClickData] = env.addSource(kafkaConsumer)
        .setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
        .assignTimestampsAndWatermarks(userLogsAssigner)
        .filter(
          (log: UserLogData) => {
            //过滤action为05和事件为02的数据
            (log.action.equalsIgnoreCase(ActionEnum.INTERACTIVE.getCode)
              &&
              log.eventType.equalsIgnoreCase(EventEnum.CLICK.getCode))
          }
        )
        .map(new UserLogClickDataMapFun)
      clickDStream.print("=====clickDStream========")

      /**
       * 4 点击数据写入ES
       *   (1)自定义ES-Sink
       */
      val clickESSink = new UserLogsClickESSink(indexName)
      clickDStream.addSink(clickESSink)
      //触发执行
      env.execute(appName)

    } catch {
      case exception: Exception=> {
        logger.error("UserLogsClickHandler.err:" + exception.getMessage)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    //应用程序名称
    val appName = "flink.UserLogsClickHandler"

    //kafka数据源topic
    val fromTopic = "travel_ods_logs"

    //kafka消费组
    val groupId = "group.UserLogsClickHandler"

    //点击日志数据输出ES(明细搜索或交互式查询)
    val indexName = QRealTimeConstant.ES_INDEX_NAME_LOG_CLICK
    //明细数据输出es
    handleLogsETL4ESJob(appName,groupID,fromTopic,indexName)
  }

}
