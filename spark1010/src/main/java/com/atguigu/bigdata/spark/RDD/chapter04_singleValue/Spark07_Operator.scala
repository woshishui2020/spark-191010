package com.atguigu.bigdata.spark.RDD.chapter04_singleValue

import org.apache.spark.{SparkConf, SparkContext}

object Spark07_Operator {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO RDD中的方法称为算子，有别于对象的方法

    // TODO 按功能分类：
    // TODO 1. Transformation:转换 (oldRDD->算子->newRDD)
    //   todo 1.1 根据类型分类
    //        1.1.1 单值类型：1,2，"abc"
    //        1.1.2 KV类型：("a",1),("b",2)
    //                reduceByKey
    // TODO 2. Action:行动  触发RDD的执行 collect，saveAsTextFile

    //
    //
    //


    sc.stop()
  }
}
