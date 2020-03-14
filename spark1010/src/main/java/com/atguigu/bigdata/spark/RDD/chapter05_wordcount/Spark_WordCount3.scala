package com.atguigu.bigdata.spark.RDD.chapter05_wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_WordCount3 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO: Spark  WordCount--3--groupByKey

    val numRDD1: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("a",3),("b",4)),2)

    val groupRDD: RDD[(String, Iterable[Int])] = numRDD1.groupByKey()

    println(groupRDD.mapValues(list => list.sum).collect().mkString(","))

    sc.stop()
  }
}
