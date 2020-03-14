package com.atguigu.bigdata.spark.RDD.chapter05_wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_WordCount4 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO: Spark  WordCount--4--aggregateByKey
    val rdd:RDD[(String, Int)] =
      sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    val aggRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(_+_,_+_)

    println(aggRDD.collect().mkString(","))

    sc.stop()
  }
}
