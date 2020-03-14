package com.atguigu.bigdata.spark.RDD.chapter14_Project.controller

import com.atguigu.bigdata.spark.RDD.chapter14_Project.common.TController
import com.atguigu.bigdata.spark.RDD.chapter14_Project.service.HotCategoryAnalysisTop10Service

// 分析控制器
class HotCategoryAnalysisTop10Controller extends TController{

  private val hotCategoryAnalysisTop10Service = new HotCategoryAnalysisTop10Service()

  override def execute(): Unit = {

    //val datas = hotCategoryAnalysisTop10Service.analysis1()
    //val datas = hotCategoryAnalysisTop10Service.analysis2()
    //val datas = hotCategoryAnalysisTop10Service.analysis3
    val datas = hotCategoryAnalysisTop10Service.analysis4()

    //println(datas.count())
    datas.foreach(println)

  }

}
