package com.atguigu.bigdata.spark.DStream.kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ReceiveAPI2 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("ReceiveAPI2").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))

    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,
      "hadoop102:2181,hadoop103:2181,hadoop104:2181",
      "test",
      Map[String, Int]("bigdata1010" -> 2)
    )

    kafkaDStream
      .flatMap{ case (k,v) => v.split(" ")}
      .map((_,1))
      .reduceByKey(_+_)
      .print()

    ssc.start()
    ssc.awaitTermination()

  }
}
