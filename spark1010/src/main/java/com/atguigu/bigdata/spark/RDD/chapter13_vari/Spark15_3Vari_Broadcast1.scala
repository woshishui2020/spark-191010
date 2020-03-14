package com.atguigu.bigdata.spark.RDD.chapter13_vari

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark15_3Vari_Broadcast1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("acc").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //TODO 广播变量 分布式共享只读变量

    val rdd1 = sc.makeRDD(List( ("a",1), ("b", 2), ("c", 3), ("d", 4) ),4)
    val list = List( ("a",4), ("b", 5), ("c", 6), ("d", 7) )

    // todo 声明广播变量(任何可序列化的类型都可以这么实现)
    val broadcast: Broadcast[List[(String, Int)]] = sc.broadcast(list)

    val resultRDD: RDD[(String, (Int, Int))] = rdd1.map {
      case (key, num) => {
        var num2 = 0
        // todo 使用广播变量(通过广播变量.value，访问该对象的值)
        for ((k, v) <- broadcast.value) {
          if (k == key) {
            num2 = v
          }
        }
        (key, (num, num2))
      }
    }
    println(resultRDD.collect().mkString(","))

    sc.stop()
  }
}
