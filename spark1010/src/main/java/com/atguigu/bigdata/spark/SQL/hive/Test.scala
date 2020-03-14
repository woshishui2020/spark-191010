package com.atguigu.bigdata.spark.SQL.hive

import org.apache.spark.sql.SparkSession

object Test {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("Test")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    spark.udf.register("cityRatio",new MyUDAF)

    spark.sql("use sparkpractice")

    spark.sql(
      """
        |select
        |    area,city_name,product_name
        |from
        |    (select
        |        click_product_id,
        |        city_id
        |    from
        |        sparkpractice.user_visit_action
        |    where
        |        click_product_id > -1) uv
        |join
        |    sparkpractice.product_info pi
        |on
        |    uv.click_product_id = pi.product_id
        |join
        |    sparkpractice.city_info ci
        |on
        |    uv.city_id = ci.city_id
      """.stripMargin).createTempView("temp1")

    //计算各个大区各个商品的点击次数及城市占比
    spark.sql(
      """
        |select
        |    area,product_name,
        |    count(*) ct,
        |    cityRatio(city_name) city_ratio
        |from
        |    temp1
        |group by
        |    area,product_name
      """.stripMargin).createTempView("temp2")

    // 取各个大区点击次数排名
    spark.sql(
      """
        |select
        |    area,product_name,
        |    ct,city_ratio,
        |    rank() over(partition by area order by ct desc) rk
        |from
        |    temp2
      """.stripMargin).createTempView("temp3")

    spark.sql(
      """
        |select
        |    area,product_name,ct,city_ratio
        |from
        |    temp3
        |where
        |    rk <= 3
      """.stripMargin).show(false)

  }
}
