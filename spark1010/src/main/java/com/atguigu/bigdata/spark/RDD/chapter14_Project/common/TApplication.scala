package com.atguigu.bigdata.spark.RDD.chapter14_Project.common

import com.atguigu.bigdata.spark.RDD.chapter14_Project.util.ProjectUtil


//通用Application
trait TApplication {

  //启动应用
  def startOnLocal(op: => Unit) : Unit = {
    start("local[*]","Application")(op)
  }

  def start(master:String,appname:String)(op: => Unit): Unit ={
    ProjectUtil.getSparkContext()

    // todo 控制抽象
    try {
      op
    } catch {
      case ex : Exception => ex.printStackTrace()
    }

    ProjectUtil.stop()
  }
}
