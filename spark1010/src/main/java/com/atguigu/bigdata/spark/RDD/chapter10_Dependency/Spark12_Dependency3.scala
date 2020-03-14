package com.atguigu.bigdata.spark.RDD.chapter10_Dependency

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_Dependency3 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("dep").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("cp")

    val lineRDD: RDD[String] = sc.textFile("input/1.txt")

    val wordRDD: RDD[String] = lineRDD.flatMap(line=>{
      println("flatMap......")
      line.split(" ")
    })

    val wordToOneRDD: RDD[(String, Int)] = wordRDD.map(word=>(word, 1))

    // todo cache会在血缘关系中增加缓存的依赖,不会切断血缘关系
    //wordToOneRDD.cache()

    //todo shuffle算子底层基本上都有缓存操作
    //val reduceRDD: RDD[(String, Int)] = wordToOneRDD.reduceByKey(_+_)

    // todo checkpoint算子会切断血缘关系，直接改变数据源（setCheckpointDir）
    wordToOneRDD.checkpoint()

    wordToOneRDD.saveAsTextFile("output3")
    println(wordToOneRDD.toDebugString)
    println("***************************")
    wordToOneRDD.saveAsTextFile("output4")

    val wordToSumRDD: RDD[(String, Int)] =
      wordToOneRDD.reduceByKey( (x, y) => x + y )

    sc.stop()
  }
}
