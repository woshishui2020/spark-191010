package com.atguigu.bigdata.spark.RDD.chapter04_singleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_Operator_Tranform1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val list : List[Int] = List(1,2,3,4)

    // TODO 算子 转换  单Value类型

    // TODO: 1. map(一次处理一个数据)
    val listRDD1: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    val listRDD2: RDD[Int] = listRDD1.map(_*2)
    listRDD2.collect().foreach(println)

    // TODO: 2.  mapPartitions(一次处理一个分区的数据)
    //   这个分区的数据处理完后，原来RDD中分区的数据才能得到释放
    //   这样可能会导致OOM，所以建议在内存空间较大的时候使用mapPartitions
    //   以提高处理效率
    val listRDD3: RDD[Int] = sc.makeRDD(list,2)
    val listRDD4: RDD[Int] = listRDD3.mapPartitions(list => list.map(_*2))
    listRDD4.collect().foreach(println)

    // TODO: 3. mapPartitionsWithIndex()带分区号
    val indexRDD: RDD[(Int, Int)] =
      listRDD3
        .mapPartitionsWithIndex(
          (index, list) =>
            list.map(num =>
              (index, num)
            )
    )
    indexRDD.collect().foreach(println)

    sc.stop()
  }
}
