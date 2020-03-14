package com.atguigu.bigdata.spark.RDD.chapter14_Project.service

import com.atguigu.bigdata.spark.RDD.chapter14_Project.bean.{HotCategory, UserVisitAction}
import com.atguigu.bigdata.spark.RDD.chapter14_Project.dao.HotCategorySessionAnalysisDao
import com.atguigu.bigdata.spark.RDD.chapter14_Project.helper.CategoryCountAccumulator
import com.atguigu.bigdata.spark.RDD.chapter14_Project.util.ProjectUtil
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.util.control.Breaks
import scala.util.control.Breaks._

class HotCategorySessionAnalysisService {

  val hotCategorySessionAnalysisDao = new HotCategorySessionAnalysisDao()

  def analysis() ={
    // TODO 获取原始数据
    val rdd: RDD[String] = hotCategorySessionAnalysisDao.textFile()
    //   todo 为了操作方便将原始数据转换为样例类对象
    val actionRDD: RDD[UserVisitAction] = rdd.map(
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

    // TODO Top10热门品类
    val acc = new CategoryCountAccumulator
    ProjectUtil.getSparkContext().register(acc)

    rdd.foreach(
      action => {
        //对用户行为类型判断
        val datas = action.split("_")
        // 点击
        if (datas(6) != "-1") {
          acc.add(datas(6), "click")
        } else if (datas(8) != "null") {
          // 下单
          val orderIds = datas(8).split(",")
          orderIds.foreach(
            id => acc.add(id, "order"))
        } else if (datas(10) != "null") {
          // 支付
          val payIds = datas(10).split(",")
          payIds.foreach(
            id => acc.add(id,"pay"))
        } else {
          Nil
        }
      }
    )

    val accValue: mutable.Map[String, HotCategory] = acc.value
    val hotCategoryMap: mutable.Iterable[HotCategory] = accValue.map(_._2)

    val categories: List[HotCategory] = hotCategoryMap.toList.sortWith(
      (left, right) => {
        if (left.clickCount > right.clickCount) {
          true
        } else if (left.clickCount == right.clickCount) {
          if (left.orderCount > right.orderCount) {
            true
          } else if (left.orderCount == right.orderCount) {
            left.payCount > right.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    ).take(10)

    // TODO 将原始数据转换:(品类_session,count)
    val categroyIdAndSessionIdToOneRDD: RDD[(String, Int)] = actionRDD.flatMap(
      action => {
        action match {
          case a: UserVisitAction if a.click_category_id != -1 =>
            List((a.click_category_id + "_" + a.session_id, 1))
          case a: UserVisitAction if a.order_category_ids != "null" =>
            val ids = a.order_category_ids.split(",")
            ids.map(id => (id + "_" + a.session_id, 1))
          case a: UserVisitAction if a.pay_category_ids != "null" =>
            val ids = a.pay_category_ids.split(",")
            ids.map(id => (id + "_" + a.session_id, 1))
          case _ => Nil
        }
      }
    )

    // TODO 数据过滤
    val filterRDD: RDD[(String, Int)] = categroyIdAndSessionIdToOneRDD.filter {
      case (key, one) => {
        val ks = key.split("_")
        val cid = ks(0) //品类ID
        var flag = false
        //判断品类id是否存在于前10
        breakable {
          for (c <- categories) {
            if (c.id == cid) {
              flag = true
              Breaks
            }
          }
        }
        flag
      }
    }

    // TODO 数据分组聚合
    val categroyAndSessionToSumRDD: RDD[(String, Int)] = filterRDD.reduceByKey(_+_)
    
    // TODO 数据结构转换
    val categroyToSessionAndSumRDD: RDD[(String, (String, Int))] = categroyAndSessionToSumRDD.map {
      case (key, sum) => {
        val ks: Array[String] = key.split("_")
        (ks(0), (ks(1), sum))
      }
    }

    // TODO 数据按key分组
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = categroyToSessionAndSumRDD.groupByKey()

    // TODO
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      datas => {
        datas.toList.sortWith(_._2 > _._2).take(10)
      }
    )
    resultRDD.collect()
  }
}
