package com.atguigu.bigdata.spark.RDD.chapter06_KeyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark08_Operator_Tranform3 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO 算子 转换  KV类型

    // todo 1. sortByKey()按照K进行排序
    //         true为升序，false降序,也可以传第二个参数改变分区数
    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
    println(rdd.sortByKey(true,2).collect().mkString(","))
    println(rdd.sortByKey(false).collect().mkString(","))

    // todo 2. join() 连接
    //   相同key的连接，不同key直接过滤掉
    //   一定会有shuffle，并且会有笛卡尔积，所以不推荐使用
    val rdd1: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")))
    val rdd2: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (4, 6)))
    val joinRDD: RDD[(Int, (String, Int))] = rdd1.join(rdd2)
    println(joinRDD.collect().mkString(",")) // (1,(a,4)),(2,(b,5))

    // todo 2. cogroup()联合
    println(rdd1.cogroup(rdd2).collect().mkString(","))


    sc.stop()
  }
}
