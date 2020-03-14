package com.atguigu.bigdata.spark.DStream.RDDWordCount

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object WordCount2 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf()
      .setAppName("wc")
      .setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val queue = new mutable.Queue[RDD[Int]]()

    val inputDStrea: InputDStream[Int] = ssc.queueStream(queue,false)

    inputDStrea.map((_,2)).reduceByKey(_+_).print()

    ssc.start()

    for (i <- 1 to 5) {
      queue += ssc.sparkContext.makeRDD(1 to 3)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()

  }
}
