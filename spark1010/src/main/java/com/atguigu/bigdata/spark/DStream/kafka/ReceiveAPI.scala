package com.atguigu.bigdata.spark.DStream.kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ReceiveAPI {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("ReceiveAPI")

    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //todo 1. 使用ReceiveAPI读取kafka数据创建DStream
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      ssc,
      "hadoop102:2181,hadoop103:2181,hadoop104:2181",
      "bigdata1010",
      Map[String, Int]("test" -> 1)
    )

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
