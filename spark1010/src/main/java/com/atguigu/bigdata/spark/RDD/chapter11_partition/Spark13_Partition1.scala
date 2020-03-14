package com.atguigu.bigdata.spark.RDD.chapter11_partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_Partition1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("dep").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    //todo 只有KV类型的RDD才有分区器，非KV类型的RDD分区的值是None
    println(rdd.partitioner)// None

    sc.stop()
  }
}
