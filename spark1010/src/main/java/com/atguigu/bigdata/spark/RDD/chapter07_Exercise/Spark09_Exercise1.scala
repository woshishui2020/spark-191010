package com.atguigu.bigdata.spark.RDD.chapter07_Exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_Exercise1 {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("exercise").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO 需求：统计出每一个省份广告被点击次数Top3--简化版
    // 1516609143867 6 7 64 16
    val rdd: RDD[String] = sc.textFile("input/agent.log")

    rdd
        .map(line => {
          val strings: Array[String] = line.split(" ")
          (strings(1)+"_"+strings(4),1)})
        .reduceByKey(_+_)
        .map(t => {
          val strings: Array[String] = t._1.split("_")
          (strings(0),(strings(1),t._2))})
        .groupByKey()
        .mapValues(t => t.toList.sortWith(_._2>_._2).take(3))
        .collect().foreach(println)

    sc.stop()
  }
}
