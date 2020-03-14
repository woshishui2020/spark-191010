package com.atguigu.bigdata.spark.RDD.chapter07_Exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_Exercise2 {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("exercise").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.makeRDD(
      List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    //println(rdd.aggregateByKey(0)((x, y) => math.max(x, y), (x, y) => x + y).collect().mkString(","))
    //println(rdd.foldByKey(0)(_ + _).collect().mkString(","))

    val rdd1: RDD[(String, Int)] = sc.makeRDD(
      List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)
    println(rdd1.combineByKey(
      x => (x, 1),
      (t: (Int, Int), num) => (t._1 + num, t._2 + 1),
      (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2)
    ).mapValues(t => t._1 / t._2).collect().mkString(","))

    val rdd2: RDD[(Int, String)] = sc.makeRDD(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))
    //println(rdd2.sortByKey(false).collect().mkString(","))

    val rdd3: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c")))
    val rdd4: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (4, 6)))
    println(rdd3.join(rdd4).collect().mkString(","))
    rdd3.cogroup(rdd4).collect().foreach(println)





    sc.stop()
  }
}
