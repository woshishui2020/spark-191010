package com.atguigu.bigdata.spark.SQL.read

import org.apache.spark.sql.{DataFrame, SparkSession}

object Reader {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("read")
      .master("local[*]")
      .getOrCreate()

    //todo 通用加载数据方式

    // 1.
    val df1: DataFrame = spark.read.json("")
    //spark.read.jdbc()

    // 2.
    val df2: DataFrame = spark.read.format("json").load("")
    //spark.read.format("jdbc").option("","").load()

    df1.show()
    df2.show()

  }
}
