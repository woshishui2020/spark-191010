package com.atguigu.bigdata.spark.DStream.Trans

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object join_WordCount {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wc")
    val ssc = new StreamingContext(conf,Seconds(3))

    //todo 端口获取DStream流数据
    val lineDStream1: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",8888)
    val lineDStream2: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    //todo 转换为KV类型
    val dStream1: DStream[(String, Int)] = lineDStream1.flatMap(_.split(" ")).map((_,1))
    val dStream2: DStream[(String, Int)] = lineDStream2.flatMap(_.split(" ")).map((_,1))

    //todo join_wordcount
    val joinDStream: DStream[(String, (Int, Int))] = dStream1.join(dStream2)

    joinDStream.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
