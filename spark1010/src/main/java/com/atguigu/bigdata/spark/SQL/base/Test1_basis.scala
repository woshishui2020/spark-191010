package com.atguigu.bigdata.spark.SQL.base

import org.apache.spark.sql.{DataFrame, SparkSession}

object Test1_basis {
  def main(args: Array[String]): Unit = {

    //todo 创建session对象
    val spark: SparkSession = SparkSession.builder()
      .appName("First")
      .master("local[*]")
      .getOrCreate()

    //todo 引入隐式转换
    import spark.implicits._

    //todo 读取文件创建DF
    val df: DataFrame = spark.read.json("input/user.json")

    //todo SQL风格的编程
    df.createTempView("user")
    spark.sql("select * from user").show()

    //todo 创建全局表
    df.createGlobalTempView("people")
    spark.sql("select * from global_temp.people").show()
    spark.newSession().sql("select count(*) from global_temp.people").show()

    println("===================")

    //todo DSL风格的编程
    df.select("username").show()
    df.filter($"age" > 20).show()

    println("===================")

    //todo 将DF转RDD
    df.rdd.foreach(println)

    println("===================")

    //todo 创建DS
    val ds = Seq(Person("qwer",18)).toDS()
    ds.show()

    //todo DS转RDD
    ds.rdd.foreach(println)

    //todo DS转DF
    ds.toDF().show()
    println("===================")
    println("===================")

    //todo DF转DS
    df.as[Person].show()

    println("===================")

    spark.stop()

  }

case class Person(username:String,age:Long)
}
