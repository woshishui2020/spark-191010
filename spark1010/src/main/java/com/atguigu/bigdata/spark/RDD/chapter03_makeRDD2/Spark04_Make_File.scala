package com.atguigu.bigdata.spark.RDD.chapter03_makeRDD2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Make_File {

  def main(args: Array[String]): Unit = {

    // TODO 从外部存储系统的数据集创建
    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // todo 从指定位置读取文件，采用的是hadoop的读取方式即按行读取
    val lineRdd: RDD[String] = sc.textFile("input")

    lineRdd.saveAsTextFile("output")

    sc.stop()

  }
}
