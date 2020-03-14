package com.atguigu.bigdata.spark.DStream.socketWordCOunt

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount2 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("wc")

    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val socketDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9000)

    socketDStream
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()

  }
}
