package com.atguigu.bigdata.spark.RDD.chapter09_Serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_serializable01 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO 序列化 Serializable
    //   Driver：算子以外的代码都是在Driver端执行
    //   Executor：算子里面的代码都是在Executor端执行
    //   初始化工作是在Driver端进行的，而实际运行程序是在Executor端进行的
    //   Driver端的数据如果想要在Executor端使用，那么就要序列化或者被采集到driver端

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val user = new User
    val emp = new emp

    // todo 分布式计算
    // Exception: Task not serializable
    // todo 匿名函数会有闭包，就会有一个闭包检测，
    //  检测是否有外部的属性或函数，如果有就需要已经被序列化
    //  或者被采集或者是样例类（推荐使用样例类，样例类会自动实现序列化）
    rdd.foreach(
      num => {
        println(num + user.age)
      }
    )

    // todo 单点计算
    //   使用collect算子，将数据采集到了Driver端，
    //   就不存在网络传输了，不需要序列化
    rdd.collect().foreach(
      num => {
        println(num + user.age)
      }
    )

    rdd.foreach(
      num => {
        println(num + emp.age)
      }
    )// 结果是乱序的
    sc.stop()
  }

  class User {
    val age:Int = 18
  }

  // todo 样例类会自动实现序列化
  case class emp() {
    val age:Int = 18
  }
}
