package com.atguigu.bigdata.spark.DStream.Trans

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Window_WordCount2 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wc")
    val ssc = new StreamingContext(conf,Seconds(3))
    ssc.checkpoint("ck3")

    //todo 端口获取DStream流数据
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",8888)

    //todo 转换为KV结构
    val wordToOneDStream: DStream[(String, Int)] = lineDStream.flatMap(_.split("")).map((_,1))

    //todo 开窗wordcount打印
    //wordToOneDStream.reduceByKeyAndWindow(_+_,Seconds(6)).print()
    wordToOneDStream.reduceByKeyAndWindow(
      (x:Int,y:Int) => x + y,
      (a:Int,b:Int) => a - b,
      Seconds(6),
      Seconds(3),
      ssc.sparkContext.defaultParallelism,
      (x: (String,Int)) => x._2 > 0).print()

    ssc.start()
    ssc.awaitTermination()

    ssc.stop(stopSparkContext = true,stopGracefully = true)

  }
}
