package com.atguigu.bigdata.spark.RDD.chapter13_vari

import org.apache.spark.{SparkConf, SparkContext}

object Spark15_3Vari2_Acc {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("acc").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO Spark 三大核心变量
    //  累加器 :分布式共享只写变量，可以将Driver端的数据传递给Executor端，
    //  也可以将Executor端传递给Driver端，而且中间没有shuffle操作，性能高
    //  不同节点的累加器无法互相访问和读取
    //  累加器的作用一般是进行数据的求和以及数据的聚合

    val rdd = sc.makeRDD(List(1,2,3,4,5))

    // todo 声明累加器
    var sum = sc.longAccumulator("sum")

    rdd.foreach(
      num =>
        sum.add(num)
    )

    // todo 获取累加器的值
    println("sum = " + sum.value) // 0

    sc.stop()

  }
}
