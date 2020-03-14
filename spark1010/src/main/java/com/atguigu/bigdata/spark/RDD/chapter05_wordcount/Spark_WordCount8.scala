package com.atguigu.bigdata.spark.RDD.chapter05_wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_WordCount8 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO: Spark  WordCount--8--countByValue
    val rdd: RDD[String] = sc.makeRDD(List("hello spark","hello spark"))

    println(rdd.flatMap(_.split(" ")).countByValue())

    sc.stop()
  }
}
