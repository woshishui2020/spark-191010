package com.atguigu.bigdata.spark.RDD.chapter05_wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_WordCount1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO: Spark  WordCount--1--groupBy
    val rdd: RDD[String] = sc.makeRDD(List("hello spark","hello spark"))

    val wordRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val groupRDD: RDD[(String, Iterable[String])] = wordRDD.groupBy(word => word)

    val wordToCount: RDD[(String, Int)] = groupRDD.map(t => (t._1,t._2.size))

    wordToCount.collect().foreach(println)

    sc.stop()
  }
}
