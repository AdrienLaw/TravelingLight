package com.adrien.bigdata.realtime.util.help

import java.util.Properties

import com.adrien.bigdata.realtime.constant.QRealTimeConstant
import com.adrien.bigdata.realtime.util.PropertyUtil
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.config.StartupMode
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Flink 相关工具包
 */
object FlinkHelper {
  //日志对象打印
  private val logger: Logger = LoggerFactory.getLogger("FlinkHelper")

  /**
   * 获取flink的流式执行环境
   * @param checkpointInterval 状态检查点之间的时间间隔（毫秒）。
   * @param tc TimeCharacteristic = EventTime
   * @param watermarkInterval
   * @return 水位间隔时间
   */
  def createStreamingEnvironment(checkpointInterval: Long,tc:TimeCharacteristic,watermarkInterval:Long): StreamExecutionEnvironment = {
    var env: StreamExecutionEnvironment = null;
    try {
      env = StreamExecutionEnvironment.getExecutionEnvironment
      //设置并行度
      env.setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)

      /**
       * 开启checkpoint
       * Param 01 = 3 * 1000l
       *     状态检查点之间的时间间隔（毫秒）。
       * Param 02 EXACTLY_ONCE
       *     检查点模式，在“EXACTLY ONCE”和“AT LEAST ONCE”之间进行选择，可以保证。
       */
      env.enableCheckpointing(checkpointInterval,CheckpointingMode.EXACTLY_ONCE)
      /**
       * 设置重新启动策略配置。配置指定在重新启动的情况下，哪个重新启动策略将用于执行图
       * Param 01 = 5
       *    固定间隔 (Fixed Delay)  的重新启动尝试次数
       * Param 02 = 10s
       *    固定间隔 (Fixed Delay) 重新启动尝试之间的延迟
       */
      env.setRestartStrategy(RestartStrategies.fixedDelayRestart(QRealTimeConstant.RESTART_ATTEMPTS,Time.seconds(10)))

      /**
       * 设置env的时间类型
       *
       * flink的TimeCharacteristic枚举定义了三类值，分别是ProcessingTime、IngestionTime、EventTime
       * 此时为 EventTime
       */
      env.setStreamTimeCharacteristic(tc)

      /**
       * 设置水位间隔时间
       * 5 * 1000l
       */
      env.getConfig.setAutoWatermarkInterval(watermarkInterval)
    }
    env
  }

  /**
   * 初始化 Flink 执行环境
   * @return
   */
  def createStreamingEnvironment(): StreamExecutionEnvironment = {
    val env: StreamExecutionEnvironment = createStreamingEnvironment(
      QRealTimeConstant.FLINK_CHECKPOINT_INTERVAL,
      TimeCharacteristic.EventTime,
      QRealTimeConstant.FLINK_WATERMARK_INTERVAL
    )
    env
  }

  /**
   * 返回flink-kafka的消费者
   * @param env
   * @param topic
   * @param groupId
   * @return
   */
  def createKafkaConsumer(env: StreamExecutionEnvironment,topic: String,groupId:String): FlinkKafkaConsumer[String] ={
    //kafka的反序列化
    val simpleStringSchema = new SimpleStringSchema()
    /**
     * 需要kafka的相关参数
     * 读取Kafka 自傲飞着配置 返回一个 Properties
     */
    val pro: Properties=PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)
    //重置消费者组
    pro.setProperty("group.id",groupId)

    /**
     * 创建kafka的消费者
     * Param 01 = topic
     *    topic 的名字
     * Param 02 = SimpleStringSchema 对象
     *    Kafka的字节消息和Flink的对象之间进行转换的反序列化程序。
     * Param 03 = property
     **/
    val kafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer(topic,simpleStringSchema,pro)
    //设置自动提交offset
    kafkaConsumer.setCommitOffsetsOnCheckpoints(true)
    kafkaConsumer
  }

  /**
   * 返回flink-kafka的消费者
   * @param env
   * @param topic
   * @param groupId
   * @param schema
   * @param sm
   * @tparam T
   * @return
   */
  def createKafkaSerDeConsumer[T: TypeInformation](env:StreamExecutionEnvironment,topic: String,groupId: String,
                                                   schema: KafkaDeserializationSchema[T],sm: StartupMode): FlinkKafkaConsumer[T] = {
    val pro: Properties = PropertyUtil.readProperties(QRealTimeConstant.KAFKA_CONSUMER_CONFIG_URL)
    //重置消费者组
    pro.setProperty("group.id",groupId)
    //创建kafka的消费者
    val kafkaConsumer: FlinkKafkaConsumer[T] = new FlinkKafkaConsumer[T](topic,schema,pro)
    //设置kafka的消费位置
    if (sm.equals(StartupMode.EARLIEST)) {
      kafkaConsumer.setStartFromEarliest()
    } else if (sm.equals(StartupMode.LATEST)) {
      kafkaConsumer.setStartFromLatest()
    }
    //设置自动提交offset
    kafkaConsumer.setCommitOffsetsOnCheckpoints(true)
    kafkaConsumer
    }

  }
}
