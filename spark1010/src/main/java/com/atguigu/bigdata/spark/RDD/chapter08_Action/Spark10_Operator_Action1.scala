package com.atguigu.bigdata.spark.RDD.chapter08_Action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_Operator_Action1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO Spark RDD 算子--Action
    //   一个行动算子会触发整个作业的执行，可以多次调用，就会产生多个作业job
    //   一个main方法就相当于一个Application，可以有多个job
    val rdd = sc.makeRDD(List(1,2,3,4))

    // todo 1. reduce()
    val i: Int = rdd.reduce(_+_)
    //println(i)

    // todo 2. count() 数量 个数
    val l: Long = rdd.count()
    //println(l)

    // todo 3. first 第一个
    val f: Int = rdd.first()
    //println(f)

    // todo 4. take()取n个数据
    val ints: Array[Int] = rdd.take(3)
    //ints.foreach(println)
    // todo 5. takeOrdered  先排序再取前n个数据组成的数组
    //println(rdd.takeOrdered(3).mkString(","))

    // todo 6. aggregate 将每个分区内的数据通过逻辑和初始值进行聚合，
    //         然后用分区间逻辑再次和初始值进行聚合
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 8)
    //val result: Int = rdd1.aggregate(0)(_+_,_+_)  // 10
    val result: Int = rdd1.aggregate(10)(_+_,_+_)
    println(result) // 100

    // todo 7. fold()（aggregate当分区内和分区间逻辑相同时）
    val f1: Int = rdd1.fold(10)(_+_)
    //println(f1)

    // todo 8. countByKey()
    val rdd2: RDD[(Int, String)] = sc.makeRDD(
      List((1, "a"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (2, "c")))
    val intToLong: collection.Map[Int, Long] = rdd2.countByKey()
    println(intToLong)  // Map(1 -> 3, 2 -> 1, 3 -> 2)

    // todo 8. countByValue()
    val rdd3: RDD[Int] = sc.makeRDD(List(1,3,3,2))
    val intToLong1: collection.Map[Int, Long] = rdd3.countByValue()
    println(intToLong1)  // Map(1 -> 1, 2 -> 1, 3 -> 2)

    // todo 9. save
    val rdd4: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)
    // 保存成Text文件
    rdd4.saveAsTextFile("output")

    // 序列化成对象保存到文件
    rdd4.saveAsObjectFile("output1")

    // 保存成Sequencefile文件
    rdd4.map((_,1)).saveAsSequenceFile("output2")




    sc.stop()
  }
}
