package com.atguigu.bigdata.spark.DStream.socketWordCOunt

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    //todo 1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("wc")
      .setMaster("local[*]")

    //todo 2.初始化SparkStreamingContext
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //todo 3.通过监控端口创建DStream，读进来的数据为一行行
    val lineStreams: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9000)

    //todo 4.数据切分
    val wordStreams: DStream[String] = lineStreams.flatMap(_.split(" "))

    //todo 5.将单词映射成元组（word,1）
    val wordAndOneStreams : DStream[(String, Int)] = wordStreams.map((_,1))

    //todo 6.将相同的单词次数做统计
    val wordAndCountStreams: DStream[(String, Int)] = wordAndOneStreams.reduceByKey(_+_)

    //todo 7.打印
    wordAndCountStreams.print()

    //todo 8.开启SparkStreamingContext
    ssc.start()
    ssc.awaitTermination()

  }
}
