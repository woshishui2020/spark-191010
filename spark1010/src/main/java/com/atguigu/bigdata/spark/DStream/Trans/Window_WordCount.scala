package com.atguigu.bigdata.spark.DStream.Trans

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Window_WordCount {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wc")
    val ssc = new StreamingContext(conf,Seconds(3))

    //todo 端口获取DStream流数据
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",8888)

    //todo 开窗
    val windowDStream: DStream[String] = lineDStream.window(Seconds(9))

    //todo wordcount
    windowDStream
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .print()

    ssc.start()
    ssc.awaitTermination()

  }
}
