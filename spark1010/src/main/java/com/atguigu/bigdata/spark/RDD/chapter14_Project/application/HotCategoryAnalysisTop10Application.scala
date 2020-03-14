package com.atguigu.bigdata.spark.RDD.chapter14_Project.application

import com.atguigu.bigdata.spark.RDD.chapter14_Project.common.TApplication
import com.atguigu.bigdata.spark.RDD.chapter14_Project.controller.HotCategoryAnalysisTop10Controller

// Top10热门品类分析

object HotCategoryAnalysisTop10Application extends App with TApplication {

  //启动应用
  startOnLocal{
    //分析逻辑（三层架构）
    val controller = new HotCategoryAnalysisTop10Controller()
    controller.execute()

  }

}
