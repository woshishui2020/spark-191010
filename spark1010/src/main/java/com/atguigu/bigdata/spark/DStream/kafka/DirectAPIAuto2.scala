package com.atguigu.bigdata.spark.DStream.kafka

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectAPIAuto2 {

  // todo 2. 创建SSC
  def getSSC: StreamingContext = {

    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("DirectAPIAuto2")

    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //todo 2.1 设置CK
    ssc.checkpoint("ck1")

    //todo 2.3 构建kafka参数信息
    val kafkaPara: Map[String, String] = Map[String,String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "bigdata191010")

    //todo 2.2 使用DirectAPI读取kafka数据创建DStream
    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaPara,
      Set("test"))

    //todo 2.4 计算wordcount并打印
    kafkaDStream
      .flatMap{
        case (k,v) => v.split(" ")}
      .map((_,1))
      .reduceByKey(_+_)
      .print()

    ssc

  }

  def main(args: Array[String]): Unit = {

    //todo 1. 获取或者创建StreamingContext
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("ck1",() => getSSC)

    ssc.start()
    ssc.awaitTermination()

  }
}
