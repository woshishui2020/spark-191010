package com.atguigu.bigdata.spark.DStream.kafka

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectAPIAuto2_3 {

  def getSSC = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DirectAPIAuto2_3")
    val ssc = new StreamingContext(conf,Seconds(3))

    ssc.checkpoint("ck2")

    val kafkaParams: Map[String, String] = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "shangguigu"
    )

    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Set("test")
    )

    kafkaDStream
      .flatMap{case (k,v) => v.split(" ")}
      .map((_,1))
      .reduceByKey(_+_)
      .print()

    ssc
  }

  def main(args: Array[String]): Unit = {

    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("ck2",() => getSSC)

    ssc.start()
    ssc.awaitTermination()

  }
}
