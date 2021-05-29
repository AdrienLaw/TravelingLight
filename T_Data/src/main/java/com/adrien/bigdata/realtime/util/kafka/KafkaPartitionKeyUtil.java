package com.adrien.bigdata.realtime.util.kafka;

import com.adrien.bigdata.realtime.util.CommonUtil;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.Random;

/**
*@Author 东哥
*@Company 千锋好程序员大数据
*@Date 2020/3/26 0026
*@Description kafka自定义分区器
**/
public class KafkaPartitionKeyUtil implements Partitioner {

    private Random random = new Random();

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }



    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int numPartitions = cluster.partitionCountForTopic(topic);

        int position = 1;
        if(null == value){
            position = numPartitions-1;
        }else{
            //String partitionInfo = key.toString();
            String md5Hex = CommonUtil.getMD5AsHex(keyBytes);
            Integer num = Math.abs(md5Hex.hashCode());

            //Long num = Long.valueOf(partitionInfo);
            Integer pos = num % numPartitions;
            position = pos.intValue();
            System.out.println("data partitions is " + position + ",num=" + num + ",numPartitions=" + numPartitions);
        }

        return position;
    }

    public static void main(String[] args) throws  Exception{

        

    }
}
