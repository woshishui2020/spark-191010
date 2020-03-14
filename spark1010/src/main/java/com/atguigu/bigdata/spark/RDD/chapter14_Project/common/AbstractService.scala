package com.atguigu.bigdata.spark.RDD.chapter14_Project.common

import com.atguigu.bigdata.spark.RDD.chapter14_Project.bean.UserVisitAction
import org.apache.spark.rdd.RDD

abstract class AbstractService {

  def getDao():TDao

  def getUserVisitActions() ={

    // todo 加载原始数据
    val dataRDD : RDD[String] = getDao.textFile()

    // todo 数据转换为样例类对象
    val actionRDD: RDD[UserVisitAction] = dataRDD.map(
      action => {
        val datas = action.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    )
    actionRDD
  }
}
