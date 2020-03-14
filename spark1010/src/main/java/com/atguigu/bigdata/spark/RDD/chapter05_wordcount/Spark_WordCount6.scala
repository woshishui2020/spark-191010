package com.atguigu.bigdata.spark.RDD.chapter05_wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_WordCount6 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO: Spark  WordCount--6--combineByKey
    val rdd:RDD[(String, Int)] =
      sc.makeRDD(List(("a", 1), ("b", 2), ("a", 3), ("b", 4), ("a", 5), ("b", 6)), 2)

    println(rdd.combineByKey(
      num => num,
      (x: Int, num) => x + num,
      (x: Int, y: Int) => x + y
    ).collect().mkString(","))

    sc.stop()
  }
}
