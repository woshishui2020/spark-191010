package com.atguigu.bigdata.spark.DStream.kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ReceiveAPI3 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("ReceiveAPI3").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))

    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      ssc,
      "hadoop102:2181,hadoop103:2181,hadoop104:2181",
      "bigdata1010",
      Map[String, Int]("test" -> 2)
    )

    kafkaDStream
      .flatMap{ case (k,v) => v.split(" ")}
      .map((_,1))
      .reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()

  }
}
