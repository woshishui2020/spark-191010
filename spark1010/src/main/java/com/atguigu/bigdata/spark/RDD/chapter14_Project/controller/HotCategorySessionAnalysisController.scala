package com.atguigu.bigdata.spark.RDD.chapter14_Project.controller

import com.atguigu.bigdata.spark.RDD.chapter14_Project.common.TController
import com.atguigu.bigdata.spark.RDD.chapter14_Project.service.HotCategorySessionAnalysisService

class HotCategorySessionAnalysisController extends TController{

  private val hotCategorySessionAnalysisService = new HotCategorySessionAnalysisService

  override def execute() = {
    val datas = hotCategorySessionAnalysisService.analysis()
    datas.foreach(println)
    //println(datas.count())
  }
}
