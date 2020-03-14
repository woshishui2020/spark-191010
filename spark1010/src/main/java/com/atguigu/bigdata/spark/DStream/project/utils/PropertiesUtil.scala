package com.atguigu.bigdata.spark.DStream.project.utils

import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {

  def load(propertiesName:String):Properties = {

    val properties = new Properties()

    properties.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName) ,
      "UTF-8"))

    properties
  }


  /*def main(args: Array[String]): Unit = {

    println(load("KOBE.properties").get("KOBE"))

  }*/

}
