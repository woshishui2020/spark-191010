package com.atguigu.bigdata.spark.SQL.hive

import org.apache.spark.sql.SparkSession

object TestHive {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("read")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    spark.sql("use gmall").show()

    spark.sql("show tables").show()

    spark.sql("select * from ads_uv_count").show()

    spark.stop()

  }
}
