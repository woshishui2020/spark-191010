package com.atguigu.bigdata.spark.RDD.chapter06_KeyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark08_Operator_Tranform2 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO 算子 转换  KV类型

    // todo 1. aggregateByKey() 按照K处理分区内和分区间逻辑
    //   aggregateByKey使用了柯里化，有多个参数列表
    //   第一个参数列表：zeroValue初始值
    //        分区内的第一条数据进行比较计算的初始值
    //   第二个参数列表
    //        seqOp:分区内计算规则
    //        comOp:分区间计算规则

    // todo  需求：每个分区取相同key的最大值后相加
    val rdd: RDD[(String, Int)] =
      sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
    //println(rdd.aggregateByKey(0)(
    //    (x, y) => math.max(x, y), (x, y) => x + y).collect().mkString(","))
    // (b,3),(a,3),(c,12)
    // 简化版
    println(rdd.aggregateByKey(0)(math.max, _ + _).collect().mkString(","))
    // 假如把第一个参数初始值改为5，结果就是(b,5),(a,5),(c,13)
    // 假如第二个参数列表中的计算规则一样的，结果就是(b,13),(a,15),(c,38)
    println(rdd.aggregateByKey(10)(_ + _, _ + _).collect().mkString(","))
    // 假如把第一个参数初始值为0，第二个参数列表中的计算规则为+，那就是一个worddcount
    println(rdd.aggregateByKey(0)(_ + _, _ + _).collect().mkString(","))

    // todo 2. foldByKey()分区内和分区间相同的aggregateByKey
    //   当aggregateByKey分区内和分区间的计算规则一样就可以替换
    println(rdd.foldByKey(0)(_ + _).collect().mkString(","))

    // todo 3. combineByKey()转换结构后分区内和分区间操作
    //     可以传 3个参数
    //   createCombiner：转换数据的结构,(只有第一个值会用)
    //   mergeValue：分区内计算规则
    //   mergeCombiners：分区间计算规则

    // todo  需求:计算相同key的数据的平均值
    val rdd1: RDD[(String, Int)] = sc.makeRDD(
      List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)
    println(rdd1.combineByKey(
      num => (num, 1), // 转换数据的结构
      (t: (Int, Int), num) => (t._1 + num, t._2 + 1), // 分区内计算规则
      (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2) // 分区间计算规则
    ).mapValues(t => t._1 / t._2).collect().mkString(","))


    sc.stop()
  }
}
