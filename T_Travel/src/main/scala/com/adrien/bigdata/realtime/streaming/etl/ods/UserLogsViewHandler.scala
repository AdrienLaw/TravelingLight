package com.adrien.bigdata.realtime.streaming.etl.ods

import com.adrien.bigdata.realtime.constant.QRealTimeConstant
import com.adrien.bigdata.realtime.enumes.ActionEnum
import com.adrien.bigdata.realtime.rdo.QRealtimeDO.{UserLogData, UserLogPageViewData}
import com.adrien.bigdata.realtime.streaming.assinger.UserLogsAssigner
import com.adrien.bigdata.realtime.streaming.funs.logs.UserLogPageViewDataMapFun
import com.adrien.bigdata.realtime.streaming.schema.{UserLogsKSchema, UserLogsPageViewKSchema}
import com.adrien.bigdata.realtime.streaming.sink.logs.UserLogsViewESSink
import com.adrien.bigdata.realtime.util.PropertyUtil
import com.adrien.bigdata.realtime.util.help.FlinkHelper
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.config.StartupMode
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaDeserializationSchema, KafkaSerializationSchema}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._
import org.apache.kafka.clients.producer.KafkaProducer
/**
 * 用户行为日志 页面浏览数据实时ETL
 * 首次处理：进行数据规范、ETL操作
 *
 * 用户对页面浏览数据实时ETL
 * -主要是过滤action为07 或 08的数据
 *    -将用户行为中的页面浏览数据明细--->打入ES
 *    -将用户行为中的页面浏览数据--->打入kafka
 */
object UserLogsViewHandler {
  val logger: Logger = LoggerFactory.getLogger("UserLogsViewHandler")

  /**
   * 用户行为日志(页面浏览行为数据)实时明细数据ETL处理
   * @param appName
   * @param groupId
   * @param fromTopic
   * @param indexName
   */
  def handleLogsETL4ESJob(appName: String,groupId: String,fromTopic: String,indexName: String): Unit = {
    try {
      /**
       * 1 Flink环境初始化
       *   流式处理的时间特征依赖(使用事件时间)
       */
      val enev: StreamExecutionEnvironment= FlinkHelper.createStreamingEnvironment()
      /**
       * 2 Kafka流式数据源
       * Kafka 消费参数
       * Kafka 消费策略
       * 创建flink消费对象FlinkKafkaConsumer
       * 用户行为日志(kafka数据)反序列化处理
       */
      val schema: KafkaDeserializationSchema[UserLogData] = new UserLogsKSchema(fromTopic)
      val kafkaConsumer: FlinkKafkaConsumer[UserLogData] = FlinkHelper.createKafkaSerDeConsumer(
        enev,fromTopic,groupId,schema,StartupMode.LATEST)
      /**
       * 3 设置事件时间提取器及水位计算
       *   方式：自定义实现AssignerWithPeriodicWatermarks 如 UserLogsAssigner
       */
      val userLogsAssigner = new UserLogsAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
      val viewDStream: DataStream[UserLogPageViewData] = enev.addSource(kafkaConsumer)
        .setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
        .assignTimestampsAndWatermarks(userLogsAssigner)
        .filter(
          (log : UserLogData) => {
            (log.action.equalsIgnoreCase(ActionEnum.PAGE_ENTER_H5.getCode)
              || log.action.equalsIgnoreCase(ActionEnum.PAGE_ENTER_NATIVE.getCode))
          }
        ).map(new UserLogPageViewDataMapFun())
      viewDStream.print()
      /**
       * 4 写入下游环节ES(具体下游环节取决于平台的技术方案和相关需求,如flink+es技术组合)
       */
      val viewESSink = new UserLogsViewESSink(indexName)
      viewDStream.addSink(viewESSink)
      enev.execute(appName)
    } catch {
      case ex: Exception => {
        logger.error("UserLogsViewHandler.err:" + ex.getMessage)
      }
    }
  }

  /**
   * 用户行为日志(页面浏览行为数据)实时明细数据ETL处理
   * @param appName
   * @param groupID
   * @param fromTopic 输入 topic
   * @param toTopic   输出 topic
   *
   */
  def handleLogsETL4KafkaJob(appName:String, groupID:String, fromTopic:String, toTopic:String): Unit = {
    try {
      /**
       * 1 Flink环境初始化
       *   流式处理的时间特征依赖(使用事件时间)
       */
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment()

      /**
       * 2 kafka流式数据源
       *   kafka消费配置参数
       *   kafka消费策略
       *   创建flink消费对象FlinkKafkaConsumer
       *   用户行为日志(kafka数据)反序列化处理
       */
      val kafkaDeserializeSchema: KafkaDeserializationSchema[UserLogData] = new UserLogsKSchema(fromTopic)
      val kafkaConsumer : FlinkKafkaConsumer[UserLogData] =
        FlinkHelper.createKafkaSerDeConsumer(env, fromTopic, groupID, kafkaDeserializeSchema, StartupMode.LATEST)
      /**
       * 2 kafka流式数据源
       *   kafka消费配置参数
       *   kafka消费策略
       *   创建flink消费对象FlinkKafkaConsumer
       *   用户行为日志(kafka数据)反序列化处理
       */
      val userLogsAssigner = new UserLogsAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)
      val viewDStream :DataStream[UserLogPageViewData] = env.addSource(kafkaConsumer)
        .setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
        .assignTimestampsAndWatermarks(userLogsAssigner)
        .filter(
          (log : UserLogData) => {
            (log.action.equalsIgnoreCase(ActionEnum.PAGE_ENTER_H5.getCode)
              || log.action.equalsIgnoreCase(ActionEnum.PAGE_ENTER_NATIVE.getCode))
          }
        )
        .map(new UserLogPageViewDataMapFun())
      viewDStream.print("viewDStream")

      /**
       * 4 写入下游环节Kafka
       *   (具体下游环节取决于平台的技术方案和相关需求,如flink+druid技术组合)
       */
      val kafkaSerializeSchema: KafkaSerializationSchema[UserLogPageViewData] = new UserLogsPageViewKSchema(toTopic)
      /**
       * 读取消费者配置
       */
      val kafkaProductPro = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_PRODUCER_CONFIG_URL)
      val viewKafkaProducer = new FlinkKafkaProducer(
        toTopic,
        kafkaSerializeSchema,
        kafkaProductPro,
        FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
      //添加时间戳
      viewKafkaProducer.setWriteTimestampToKafka(true)
      //添加sink
      viewDStream.addSink(viewKafkaProducer)
      //触发执行
      env.execute(appName)
    } catch {
      case exception: Exception => {
        logger.error("UserLogsViewHandler.err:" + exception.getMessage)
      }
    }
  }

  def main(args: Array[String]): Unit = {

  }
}
