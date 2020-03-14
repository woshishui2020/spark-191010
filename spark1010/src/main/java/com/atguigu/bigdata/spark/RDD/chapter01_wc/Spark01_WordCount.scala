package com.atguigu.bigdata.spark.RDD.chapter01_wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {

  def main(args: Array[String]): Unit = {

    // 1.创建SparkConf
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")

    // 2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    // 3.读取指定位置文件
    val lineRDD: RDD[String] = sc.textFile("input")

    // 4.切割成单词
    val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))

    // 5.数据格式转换
    val wordToOneRDD: RDD[(String, Int)] = wordRDD.map(word => (word,1))

    // 6.聚合
    val sumRDD: RDD[(String, Int)] = wordToOneRDD.reduceByKey((x, y) => x + y)

    // 7.打印遍历
    //sumRDD.collect().foreach(println)

    // 简化版
    sc
      .textFile("input")
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .collect()
      .foreach(println)


    // 8.关闭连接
    sc.stop()
  }
}
