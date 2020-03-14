package com.atguigu.bigdata.spark.DStream.kafka

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectAPIAuto1 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("DirectAPIAuto1")

    val ssc = new StreamingContext(sparkConf,Seconds(3))

    ssc.checkpoint("ck")

    //todo 1.1 构建kafka参数信息
    val kafkaPara: Map[String, String] = Map[String,String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "bigdata191010")

    //todo 1. 使用DirectAPI读取kafka数据创建DStream
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaPara,
      Set("test"))

    //todo 2. 计算wordcount并打印
    kafkaDStream
      .flatMap{
        case (k,v) => v.split(" ")}
      .map((_,1))
      .reduceByKey(_+_)
      .print()

    ssc.start()
    ssc.awaitTermination()

  }
}
