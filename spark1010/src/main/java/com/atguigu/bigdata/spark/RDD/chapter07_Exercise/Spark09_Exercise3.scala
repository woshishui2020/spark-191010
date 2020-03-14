package com.atguigu.bigdata.spark.RDD.chapter07_Exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_Exercise3 {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("exercise").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(
      List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)


    sc.stop()
  }
}
