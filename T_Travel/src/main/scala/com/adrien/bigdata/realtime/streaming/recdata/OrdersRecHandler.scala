package com.adrien.bigdata.realtime.streaming.recdata

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.adrien.bigdata.realtime.constant.QRealTimeConstant
import com.adrien.bigdata.realtime.rdo.QRealtimeDO.OrderDetailData
import com.adrien.bigdata.realtime.streaming.assinger.OrdersPeriodicAssigner
import com.adrien.bigdata.realtime.streaming.funs.orders.OrdersETLFun.OrderDetailDataMapFun
import com.adrien.bigdata.realtime.util.help.FlinkHelper
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.parquet.hadoop.ParquetWriter

/**
 * 订单数据落地为 HDFS 中的 parquet 文件
 */
object OrdersRecHandler {
  //日志记录
  val logger: Logger = LoggerFactory.getLogger("OrdersRecHandler")

  /**
   * 旅游订单实时数据采集落地（列文件格式）
   * @param appName 任务名称
   * @param groupId
   * @param formTopic
   * @param output
   * @param bucketCheckInterval
   */
  def handlerParquet2HDFS(appName: String,groupId: String ,formTopic: String,
                          output: String,bucketCheckInterval: Long): Unit = {
    try {
      /**
       * 1 Flink环境初始化
       *   流式处理的时间特征依赖(使用事件时间)
       *   需设置：并行度、检查点模式、重启策略、时间类型、水位间隔时间
       */
      val env: StreamExecutionEnvironment = FlinkHelper.createStreamingEnvironment()
      /**
       * 2 kafka流式数据源
       *   kafka消费配置参数
       *   kafka消费策略
       */
      val kafkaConsumer: FlinkKafkaConsumer[String] = FlinkHelper.createKafkaConsumer(env,formTopic,groupId)

      /**
       * 3 旅游产品订单数据
       *   (1) 设置水位及事件时间提取(如果时间语义为事件时间的话)
       *   (2)原始明细数据转换操作(json->业务对象OrderDetailData)
       *
       *   Param = 51
       *      flink最大乱序时间
       */
      val orderPeriodAssigner = new OrdersPeriodicAssigner(QRealTimeConstant.FLINK_WATERMARK_MAXOUTOFORDERNESS)

      /**
       *
       */
      val orderDetailDStream :DataStream[OrderDetailData] = env.addSource(kafkaConsumer)
        .setParallelism(QRealTimeConstant.DEF_LOCAL_PARALLELISM)
        .map(new OrderDetailDataMapFun())
        .assignTimestampsAndWatermarks(orderPeriodAssigner)
      orderDetailDStream.print("order.orderDStream---")

      //4 数据实时采集落地

      //数据落地路径
      val outputPath:Path = new Path(output)
      //分桶检查点时间间隔
      val bucketCheckInl = TimeUnit.SECONDS.toMillis(bucketCheckInterval)

      /**
       * 数据分桶分配器 yyyyMMDDHH
       *
       * BasePathBucketAssigner	不分桶，所有文件写到根目录
       * DateTimeBucketAssigner	基于系统时间分桶
       */
      val bucketAssigner: BucketAssigner[OrderDetailData,String] = new DateTimeBucketAssigner(QRealTimeConstant.FORMATTER_YYYYMMDD)
      /**
       * 4 数据实时采集落地
       * 需要引入Flink-parquet的依赖
       *
       * StreamingFileSink提供了两个静态方法来构造相应的sink，
       * * forRowFormat用来构造写入行格式数据的sink，
       * * forBulkFormat方法用来构造写入列格式数据的sink，
       */
      val HDFSParquetSink: StreamingFileSink[OrderDetailData] = StreamingFileSink.forBulkFormat(
        outputPath,ParquetAvroWriters.forReflectRecord(classOf[OrderDetailData])
      ).withBucketAssigner(bucketAssigner)
        .withBucketCheckInterval(bucketCheckInl)
        .build()
      //将数据流持久化到指定sink
      orderDetailDStream.addSink(HDFSParquetSink)
      //触发执行
      env.execute(appName)
    } catch {
      case ex: Exception => {
        logger.error("OrdersRecHandler.err:" + ex.getMessage)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    //行数据输出测试
    handlerParquet2HDFS("orderparquet2hdfs",
      "logs-group-id3",
      "travel_ods_orders",
      "hdfs://hadoop101:9000/travel/orders_detail/",
      60)
  }


}
