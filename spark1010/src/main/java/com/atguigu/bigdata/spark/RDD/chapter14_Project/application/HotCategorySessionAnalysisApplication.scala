package com.atguigu.bigdata.spark.RDD.chapter14_Project.application

import com.atguigu.bigdata.spark.RDD.chapter14_Project.common.TApplication
import com.atguigu.bigdata.spark.RDD.chapter14_Project.controller.HotCategorySessionAnalysisController

object HotCategorySessionAnalysisApplication extends App with TApplication{

  startOnLocal{
    val controller = new HotCategorySessionAnalysisController
    controller.execute()
  }
}
