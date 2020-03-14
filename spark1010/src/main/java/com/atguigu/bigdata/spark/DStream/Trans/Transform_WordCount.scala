package com.atguigu.bigdata.spark.DStream.Trans

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Transform_WordCount {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wc")
    val ssc = new StreamingContext(conf,Seconds(3))

    //todo 端口获取DStream数据
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    //todo wordcount
    val wordToCountDStream: DStream[(String, Int)] = lineDStream.transform(
      rdd => {
        rdd.flatMap(_.split(" "))
          .map((_, 1))
          .reduceByKey(_ + _)
      }
    )

    wordToCountDStream.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
