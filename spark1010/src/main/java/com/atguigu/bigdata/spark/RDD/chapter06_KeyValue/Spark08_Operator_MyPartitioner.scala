package com.atguigu.bigdata.spark.RDD.chapter06_KeyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark08_Operator_MyPartitioner {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO 算子 转换  KV类型

    // todo  partitionBy() 自定义分区器
    val numRDD: RDD[(String, Int)] = sc.makeRDD(
        List(("nba",1),("cba",2),("cba",1),("nba",3)),2)

    val myRDD: RDD[(String, Int)] = numRDD.partitionBy(new MyPartitioner(3))
    println(myRDD.mapPartitionsWithIndex(
      (index, list) =>
        list.map((index, _))
    ).collect().mkString(","))

    sc.stop()
  }

  // 自定义分区器
  class MyPartitioner(num:Int) extends Partitioner {
    override def numPartitions: Int = num

    override def getPartition(key: Any): Int = {
      key match {
        case null => 0
        case s:String if "nba".equals(s) => 1
        case s:String if "cba".equals(s) => 2
        case _ => 0
      }
    }
  }
}
