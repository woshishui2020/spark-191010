package com.atguigu.bigdata.spark.SQL.base

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Test2_rddToDF {
  def main(args: Array[String]): Unit = {

    //todo 创建session对象
    val spark: SparkSession = SparkSession.builder()
      .appName("First")
      .master("local[*]")
      .getOrCreate()

    val rdd: RDD[String] = spark.sparkContext.textFile("input/3.txt")

    val rowRDD: RDD[Row] = rdd.map {
      line => {
        val str: Array[String] = line.split(",")
        Row(str(0), str(1).toInt)
      }
    }

    val structType = StructType(StructField("name",StringType)::StructField("age",IntegerType)::Nil)

    val dataFrame: DataFrame = spark.createDataFrame(rowRDD,structType)

    dataFrame.show()

    spark.stop()

  }
}
