### 项目实体封装
一、编写项目的常量类:
``` com.adrien.bigdata.realtime.constant.QRealTimeConstant.scala ```

二、实体对象的封装，具体实体对象有如下:
``` yaml
用于封装产品维度数据：
    com.adrien.bigdata.realtime.streaming.rdo.QRealTimeDimDO.scala
封装原数数据、dw数据、聚合维度和聚合度量值等:
    com.adrien.bigdata.realtime.streaming.rdo.QRealtimeDO.scala
用于封装产品维度数据类型：
    com.adrien.bigdata.realtime.streaming.rdo.typeinformation.QRealTimeDimTypeInformation.scala
```

三、项目工具封装
```$yaml
1、时间工具:
    com.adrien.bigdata.realtime.util.DateUtil.scala
2、ES工具封装:
    com.adrien.bigdata.realtime.util.es.ESConfigUtil.scala
    com.adrien.bigdata.realtime.util.es.ES6ClientUtil.scala
3、通用工具:
    com.adrien.bigdata.realtime.util.CommonUtil.scala
4、properties工具:
    com.adrien.bigdata.realtime.util.PropertyUtil.scala
5、json工具:
    com.adrien.bigdata.realtime.util.JsonUtil.scala
6、druid连接池:
    com.adrien.bigdata.realtime.util.DBDruid.scala
7、redis连接池:
    com.adrien.bigdata.realtime.util.RedisCache.scala
8、kafka分区工具:
    com.adrien.bigdata.realtime.util.KafkaPartitionKeyUtil.scala
9、flink工具:
    com.adrien.bigdata.realtime.util.help.FlinkHelper.scala
```

四、实时ETL
1 订单明细数据落地为hdfs中parquet格式的文件
```$yaml
1、订单明细数据落地为hdfs中parquet格式的文件
    -以parquet格式存储到hdfs中 
    -路径为hdfs://host:port/项目名/表名/分桶目录/文件数据 
    -供离线数仓部分使用
    com.adrien.bigdata.realtime.streaming.recdata.OrdersRecHandler.scala
```

3.4.2 用户行为数据实时ETL
```$yaml
1、用户交互行为点击事件明细数据实时ETL后打入ES 
    -过滤出action为05和eventtype为02的数据 
    -设置最大乱序数据时间5s
com.adrien.bigdata.realtime.enumes.EventEnum.java

com.adrien.bigdata.realtime.enumes.ActionEnum.java

```