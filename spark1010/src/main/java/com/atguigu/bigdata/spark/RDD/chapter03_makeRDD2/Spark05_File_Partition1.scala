package com.atguigu.bigdata.spark.RDD.chapter03_makeRDD2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_File_Partition1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // todo 从指定位置读取文件，采用的是hadoop的读取方式即按行读取
    // todo textFile方法可以设置文件的分区，如果不设置采用默认值
    //      默认值：minPartitions :Int = defaultminPartitions
    //          defaultminPartitions :Int = math.min(defaultParallelism,2)
    //              defaultParallelism是CPU核数
    //val lineRdd: RDD[String] = sc.textFile("input")
    val lineRdd: RDD[String] = sc.textFile("input",2)

    lineRdd.saveAsTextFile("output")

    sc.stop()


  }
}
