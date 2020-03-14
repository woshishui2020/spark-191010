package com.atguigu.bigdata.spark.DStream.Receiver

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestReceiver {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("receive")

    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val dStream: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("hadoop102",9999))

    dStream.flatMap(_.split("")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()

    ssc.awaitTermination()

  }
}
