package com.atguigu.bigdata.spark.RDD.chapter14_Project.controller

import com.atguigu.bigdata.spark.RDD.chapter14_Project.common.TController
import com.atguigu.bigdata.spark.RDD.chapter14_Project.service.PageFlowAnalysisService

class PageFlowAnalysisController extends TController {

  private val pageFlowAnalysisService = new PageFlowAnalysisService

  override def execute() = {

    val datas = pageFlowAnalysisService.analysis3()

  }
}
