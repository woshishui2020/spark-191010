package com.atguigu.bigdata.spark.RDD.chapter03_makeRDD2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_File_Partition2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // todo 从指定位置读取文件，采用的是hadoop的读取方式即按行读取

    // todo textFile方法可以设置文件的分区，如果不设置采用默认值
    //   textFile指定分区指的是可以的最小的分区数，所以意味着可能会更大

    val lineRdd: RDD[String] = sc.textFile("input/3.txt",3)

    lineRdd.saveAsTextFile("output")

    sc.stop()


  }
}
