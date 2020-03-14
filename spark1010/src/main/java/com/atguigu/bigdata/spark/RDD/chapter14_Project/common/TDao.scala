package com.atguigu.bigdata.spark.RDD.chapter14_Project.common

import com.atguigu.bigdata.spark.RDD.chapter14_Project.util.ProjectUtil
import org.apache.spark.rdd.RDD

trait TDao {

  def textFile(): RDD[String] = {
    ProjectUtil.getSparkContext().textFile("input/user_visit_action.txt")
  }

}
