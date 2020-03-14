package com.atguigu.bigdata.spark.RDD.chapter12_JSON

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark14_JSON1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("JSON").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //todo JSON:JavaScript Object Notation
    // 特殊标记的JS对象（没有类型）
    // {"username":"zhangsan","age":20}
    // [{"username":"zhangsan","age":20}]

    //todo Spark中读取文件采用hadoop按行读取，所以读取的一行
    //   数据应该符合JSON格式，而不是整个文件符JSON格式

    val jsonRDD: RDD[String] = sc.textFile("input/user.json")

    import scala.util.parsing.json.JSON
    val resultRDD: RDD[Option[Any]] = jsonRDD.map(JSON.parseFull)

    resultRDD.collect().foreach(println)

    sc.stop()

  }
}
