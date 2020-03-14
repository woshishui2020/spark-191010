package com.atguigu.bigdata.spark.SQL.hive

import java.text.DecimalFormat

case class CityRatio(cityName:String,ratio:Double) {

  private val format = new DecimalFormat("0.0%")

  override def toString: String = {
    s"$cityName:${format.format(ratio)}"

  }
}
