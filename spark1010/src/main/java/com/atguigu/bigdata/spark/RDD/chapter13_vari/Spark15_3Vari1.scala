package com.atguigu.bigdata.spark.RDD.chapter13_vari

import org.apache.spark.{SparkConf, SparkContext}

object Spark15_3Vari1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("acc").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO Spark 三大核心变量
    //   RDD 封装逻辑
    //   累加器 广播变量 封装数据（共享变量）

    //todo 累加器
    // sum为算子foreach之外的属性属于Driver端，而遍历求和的操作是在
    // Executor端执行的，RDD没有提供方法将数据从Executor端传递到
    // Driver端，所以最后sum的结果仍为0。想要解决这个问题可以采用
    // 分布式只写变量->累加器

    val rdd = sc.makeRDD(List(1,2,3,4,5))
    var sum = 0

    rdd.foreach(
      num =>
        sum = sum + num
    )

    println("sum = " + sum) // 0


  }
}
