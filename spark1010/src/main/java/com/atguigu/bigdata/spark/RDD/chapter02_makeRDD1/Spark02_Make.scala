package com.atguigu.bigdata.spark.RDD.chapter02_makeRDD1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Make {

  def main(args: Array[String]): Unit = {

    // TODO 从集合(内存)中创建RDD(Seq)

    // 环境对象
    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 创建集合
    val list = List(1,2,3,4)

    // 创建RDD
    val rdd: RDD[Int] = sc.makeRDD(list)

    // 执行RDD(action)
    rdd.collect()

    // 打印
    rdd.foreach(println)

    sc.stop()
  }
}
