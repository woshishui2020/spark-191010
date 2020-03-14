package com.atguigu.bigdata.spark.SQL.read

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object jdbcReader {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("read")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //todo jdbc参数
    val properties = new Properties()
    properties.put("user","root")
    properties.put("password","root")

    //todo 读取MySQL数据方式1
    val jdbcDF1: DataFrame = spark.read.jdbc(
      "jdbc:mysql://hadoop102:3306/gmall","base_category1",properties)
    jdbcDF1.show()

    //todo 读取MySQL数据方式2
    val jdbcDF2: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/gmall")
      .option("dbtable", " base_category2")
      .option("user", "root")
      .option("password", "root")
      .load()
    jdbcDF2.show()

    spark.stop()

  }
}
