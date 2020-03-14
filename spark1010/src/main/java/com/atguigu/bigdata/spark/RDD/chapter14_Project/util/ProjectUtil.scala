package com.atguigu.bigdata.spark.RDD.chapter14_Project.util

import org.apache.spark.{SparkConf, SparkContext}

object ProjectUtil {

  //TheradLocal可以在内存中共享数据
  private val scLocal: ThreadLocal[SparkContext] = new ThreadLocal[SparkContext]()

  //获取Spark环境对象
  def getSparkContext() = {
    //从内存中获取环境对象
    var sc: SparkContext = scLocal.get()
    if (sc == null) {
      val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Application")
      val sc = new SparkContext(conf)
      //将新创建的环境对象保存到内存中
      scLocal.set(sc)
    }
    sc
  }

  def stop(): Unit ={
    getSparkContext().stop()
  }

}
