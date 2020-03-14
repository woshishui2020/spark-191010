package com.atguigu.bigdata.spark.RDD.chapter10_Dependency

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_Dependency1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("dep").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.textFile("input/1.txt")

    val wordRDD: RDD[String] = lineRDD.flatMap(line=>{
      println("flatMap......")
      line.split(" ")
    })

    val wordToOneRDD: RDD[(String, Int)] = wordRDD.map(word=>(word, 1))
    // TODO RDD的cache方法默认只能放置在内存中
    //   如果想要将数据保存到磁盘中，那么可以采用persist方法
    wordToOneRDD.cache()
    //wordToOneRDD.persist(StorageLevel.MEMORY_AND_DISK)
    //  todo RDD中不存储数据的，所以同一个RDD执行多个不同分支的操作，
    //   会从头多次执行数据䣌加载所以为了提高效率，可以将RDD计算的结果保存到缓
    //   存中方便下一回的使用。如果RDD执行过程中出现的错误，那么可以将那些执行时
    //   间长或数据非常重要的缓存起来，防止数据丢失，并且提高效率

    wordToOneRDD.saveAsTextFile("output1")
    println("***************************")
    wordToOneRDD.saveAsTextFile("output2")

    val wordToSumRDD: RDD[(String, Int)] =
      wordToOneRDD.reduceByKey( (x, y) => x + y )

    sc.stop()
  }
}
