package com.atguigu.bigdata.spark.SQL.udaf

import com.atguigu.bigdata.spark.SQL.{MyAvg, MyMax}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TestUDAF {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("First")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //todo 注册函数
    spark.udf.register("myavg",new MyAvg)

    spark.udf.register("mymax",new MyMax)

    //todo 读取JSON文件创建DF
    val df: DataFrame = spark.read.json("input/user.json")

    //todo 计算
    df.createTempView("user")
    //spark.sql("select myavg(age) from user").show()

    spark.sql("select mymax(age) from user").show()

    spark.stop()

  }
}
