package com.atguigu.bigdata.spark.DStream.RDDWordCount

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object WordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf()
      .setAppName("wc")
      .setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //todo 1.创建一个RDD队列
    val rddQueue = new mutable.Queue[RDD[Int]]()

    //todo 2.创建QueueDStream
    val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue,false)

    //todo 3.处理队列中的RDD数据并打印
    inputStream.map((_,1)).reduceByKey(_+_).print()

    //todo 4.启动
    ssc.start()

    //todo 5.循环创建并向RDD队列中放入RDD

    for ( i <- 1 to 3 ) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 5)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()

  }
}
