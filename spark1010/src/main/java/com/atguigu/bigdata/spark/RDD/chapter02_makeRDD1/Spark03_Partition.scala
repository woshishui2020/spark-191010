package com.atguigu.bigdata.spark.RDD.chapter02_makeRDD1

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Partition {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val list = List(1,2,3,4)

    // todo 默认数据的分区数量为机器的核数，可以动态改变
    //val rdd = sc.makeRDD(list)
    val rdd = sc.makeRDD(list,3)

    // 将数据保存到文件中
    rdd.saveAsTextFile("output")

    sc.stop()
  }
}
