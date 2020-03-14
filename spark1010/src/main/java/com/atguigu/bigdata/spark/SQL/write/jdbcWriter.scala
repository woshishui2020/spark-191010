package com.atguigu.bigdata.spark.SQL.write

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object jdbcWriter {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("read")
      .master("local[*]")
      .getOrCreate()

    //todo 创建DF
    val df: DataFrame = spark.read.json("input/user.json")

    //todo 将DF数据写入到MySQL
    df.write.format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/gmall")
      .option("dbtable", " user")
      .option("user", "root")
      .option("password", "root")
      .save()

    spark.stop()

  }
}
