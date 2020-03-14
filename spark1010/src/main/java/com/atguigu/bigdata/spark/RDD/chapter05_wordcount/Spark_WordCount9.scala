package com.atguigu.bigdata.spark.RDD.chapter05_wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_WordCount9 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO: Spark  WordCount--9--aggregate
    val rdd: RDD[String] = sc.makeRDD(List("hello spark","hello spark"))

    println(rdd
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .groupByKey()
      .mapValues(
        t =>
          t.aggregate(0)(_ + _, _ + _)
      ).collect().mkString(","))

    /*rdd
      .flatMap(_.split(" "))
      .map(word => Map((word,1)))
      .reduce((m1,m2) => m1.foldLeft(m2))*/


    sc.stop()
  }
}
