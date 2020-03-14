package com.atguigu.bigdata.spark.RDD.chapter06_KeyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark08_Operator_Tranform1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO 算子 转换  KV类型

    // todo 1. partitionBy()  分区器
    //   Spark默认分区器为HashPartitioner
    val numRDD: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("c",3),("d",4)),2)
    println(numRDD.mapPartitionsWithIndex(
      (index, list) =>
        list.map((index, _))
    ).collect().mkString(","))

    val hashRDD: RDD[(String, Int)] = numRDD.partitionBy(new HashPartitioner(3))
    println(hashRDD.mapPartitionsWithIndex(
      (index, list) =>
        list.map((index, _))
    ).collect().mkString(","))

    // todo 2. reduceByKey可以将相同的key对应的value进行reduce操作
    val numRDD1: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("a",3),("b",4)),2)
    val reduceByKeyRDD: RDD[(String, Int)] = numRDD1.reduceByKey(_+_)
    println(reduceByKeyRDD.collect().mkString(","))

    // todo 3. groupByKey只能针对于key进行分组
    val groupRDD: RDD[(String, Iterable[Int])] = numRDD1.groupByKey()
    println(groupRDD.mapValues(list => list.sum).collect().mkString(","))

    // todo 4. reduceByKey和groupByKey区别
    //   reduceByKey：在shuffle之前有分区内的combine（预聚合）操作，减少了shuffle落盘的数据量
    //                减少了IO的次数，提高了性能
    //   groupByKey：按照key进行分组，直接进行shuffle

    sc.stop()
  }
}
