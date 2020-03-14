package com.atguigu.bigdata.spark.RDD.chapter07_Exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_Exercise {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("exercise").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO 需求：统计出每一个省份广告被点击次数Top3

    // todo 1. 获取原始日志数据
    val rdd: RDD[String] = sc.textFile("input/agent.log")

    // todo 2. 将日志数据转换结构(a,b,c,d,e)=>(省份_广告，1)
    val rdd1: RDD[(String, Int)] = rdd.map(
      line => {
        val datas: Array[String] = line.split(" ")
        (datas(1) + "_" + datas(4), 1)
      }
    )
    //println(rdd1.collect().mkString(","))
    //(6_16,1),(9_18,1),(1_12,1),(2_9,1)...

    // todo 3. 将转换后的数据进行分组聚合
    val rdd2: RDD[(String, Int)] = rdd1.reduceByKey(_+_)
    //println(rdd2.collect().mkString(","))
    //(6_12,13),(7_6,22),(8_10,15),(3_20,16)

    // todo 4. 将聚合的结果进行结构转换
    val rdd3: RDD[(String, (String,Int))] = rdd2.map { t => {
      val strings: Array[String] = t._1.split("_")
      (strings(0), (strings(1),t._2))
    }
    }

    // todo 5. 分组
    val rdd4: RDD[(String, Iterable[(String, Int)])] = rdd3.groupByKey()

    // todo 6. 排序取Top3
    val rdd5: RDD[(String, List[(String, Int)])] = rdd4.mapValues(t => t.toList.sortWith(_._2>_._2).take(3))

    // todo 7. 打印
    println(rdd5.collect().mkString("\n"))
    sc.stop()
  }
}
