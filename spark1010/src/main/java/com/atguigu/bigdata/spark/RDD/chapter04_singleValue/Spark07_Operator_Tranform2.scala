package com.atguigu.bigdata.spark.RDD.chapter04_singleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_Operator_Tranform2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val list = List(List(1,2),List(3,4),List(5,6),List(7))
    val lsitRDD: RDD[List[Int]] = sc.makeRDD(list,2)

    // TODO 算子  转换  单Value类型

    // TODO: 1. flatMap
    //lsitRDD.flatMap(list => list).collect().foreach(println)

    // TODO: 2. glom 分区转换数组
    val listRDD: RDD[Int] = sc.makeRDD(1 to 4,2)
    val maxRDD: RDD[Int] = listRDD.glom().map(_.max)
    //println(maxRDD.collect().sum)

    // TODO: 3. groupBy 分组
    listRDD.groupBy(num => num % 2==0).collect().foreach(println)

    val stringRDD: RDD[String] = sc.makeRDD(List("hello","hive","hadoop","spark","scala"))
    stringRDD.groupBy(s => s.substring(0,1)).collect().foreach(println)

    // TODO: 4. filter()过滤（可能会有数据倾斜）
    val filterRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    println(filterRDD.
      filter(a => a % 2 == 0).collect().mkString(","))

    sc.stop()
  }
}
