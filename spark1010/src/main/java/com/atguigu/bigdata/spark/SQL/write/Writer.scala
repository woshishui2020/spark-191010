package com.atguigu.bigdata.spark.SQL.write

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Writer {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("write")
      .master("local[*]")
      .getOrCreate()


    //todo 创建DF
    val df: DataFrame = spark.read.json("")

    //TODO 将DF写出(写出保存数据不再是session对象，而是DF对象)

    //todo 1.通过write直接指定写出的文件格式
    df.write.json("")
    //df.write.jdbc()
    //todo 1.1 文件保存选项Append Overwrite ErrorIfExists Ignore
    df.write.mode(SaveMode.Append).json("")

    //todo 2.通过format指定保存的文件格式（也可以有文件保存选项）
    df.write.format("json").save("")
    //df.write.format("jdbc").option("","").save()

    spark.stop()

  }
}
