package com.atguigu.bigdata.spark.RDD.chapter06_doubleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_Operator_Tranform {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO 算子 转换  双Value类型

    // todo: 1. intersection()交集
    val rdd1: RDD[Int] = sc.makeRDD(1 to 4)
    val rdd2: RDD[Int] = sc.makeRDD(3 to 6)
    //println(rdd1.intersection(rdd2).collect().mkString(","))

    // todo: 2. union()并集
    //println(rdd1.union(rdd2).collect().mkString(","))

    // todo: 3. subtract()差集
    //println(rdd1.subtract(rdd2).collect().mkString(","))
    //println(rdd2.subtract(rdd1).collect().mkString(","))

    // todo: 4. zip()拉链
    //  元素个数不同，不能拉链
    //  分区数不同，不能拉链
    val rdd3: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    val rdd4: RDD[Int] = sc.makeRDD(List(3,4,5,6),2)
    val rdd5: RDD[Int] = sc.makeRDD(List(7,8),2)
    val rdd6: RDD[Int] = sc.makeRDD(List(9,10,11,12),3)

    println(rdd3.zip(rdd4).collect().mkString(","))
    println(rdd3.zip(rdd5).collect().mkString(",")) // Error
    println(rdd3.zip(rdd6).collect().mkString(",")) // Error

    sc.stop()
  }
}
