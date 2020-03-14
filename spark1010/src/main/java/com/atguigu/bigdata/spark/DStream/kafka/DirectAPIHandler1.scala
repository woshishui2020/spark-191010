package com.atguigu.bigdata.spark.DStream.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectAPIHandler1 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("DirectAPIHandler1")

    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //todo 2. 构建kafka参数信息
    val kafkaPara: Map[String, String] = Map[String,String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "shanghai")

    //todo 2.1上一次消费的位置信息
    val fromOffsets: Map[TopicAndPartition, Long] = Map[TopicAndPartition,Long](
      TopicAndPartition("direct",0) -> 3L,
      TopicAndPartition("direct",1) -> 2L
    )

    //todo 1. 使用DirectAPI读取kafka数据创建DStream
    val kafkaDStream: InputDStream[MessageAndMetadata[String, String]] =
      KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,MessageAndMetadata[String,String]](
        ssc,
        kafkaPara,
        fromOffsets,
        (m: MessageAndMetadata[String, String]) => m)

    //todo 3. 定义空数组用于存放每个批次的offset信息
    var offsetRanges = Array.empty[OffsetRange]

    //todo 2. 获取流所消费到的当前offset并打印
    kafkaDStream.transform {
      rdd =>
        //todo 获取当前批次数据的offset
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
      //todo 取每行数据的value
      .map(_.message())
      .foreachRDD {
        rdd =>
          for (o <- offsetRanges) {
            println(s"${o.topic}->${o.partition}->${o.fromOffset}->${o.untilOffset}")
          }
          //todo 打印当前批次的数据
          rdd.foreach(println)
      }

    ssc.start()
    ssc.awaitTermination()

  }
}
