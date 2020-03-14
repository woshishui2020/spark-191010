package com.atguigu.bigdata.spark.RDD.chapter05_wordcount

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark_WordCount11 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO: Spark  WordCount--11--累加器

    val rdd= sc.makeRDD(List("hello","hadoop","hello","spark","hive"))
    
    //使用累加器
    val accumulator = new MyAccumulator
    //注册累加器
    sc.register(accumulator)

    rdd.foreach(
      word => {
        accumulator.add(word)
      }
    )

    //获取累加器的值
    println(accumulator.value)

    sc.stop()
  }

  // todo 自定义累加器
  class MyAccumulator extends AccumulatorV2[String,mutable.Map[String,Long]] {

    var map : mutable.Map[String,Long] = mutable.Map()

    //累加器是否为初始状态
    override def isZero: Boolean = {
      map.isEmpty
    }

    //复制累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator
    }

    //重置累加器
    override def reset(): Unit = {
      map.clear()
    }

    //向累加器中增加数据（In）
    override def add(word: String): Unit = {

      //查询map中是否存在相同的单词，如果有那么数量+1
      //没有如果那么在map中增加这个单词
      map(word) = map.getOrElse(word,0L) + 1L
    }

    //合并累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = map
      val map2 = other.value

      map = map1.foldLeft(map2)(
        (innerMap,kv) => {
          innerMap(kv._1) = innerMap.getOrElse(kv._1,0L) + kv._2
          innerMap
        }
      )
    }

    //返回累加器的结果（Out）
    override def value: mutable.Map[String, Long] = map
  }
}
