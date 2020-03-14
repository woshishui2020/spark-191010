package com.atguigu.bigdata.spark.RDD.chapter14_Project.service

import com.atguigu.bigdata.spark.RDD.chapter14_Project.bean.HotCategory
import com.atguigu.bigdata.spark.RDD.chapter14_Project.dao.HotCategoryAnalysisTop10Dao
import com.atguigu.bigdata.spark.RDD.chapter14_Project.helper.CategoryCountAccumulator
import com.atguigu.bigdata.spark.RDD.chapter14_Project.util.ProjectUtil
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class HotCategoryAnalysisTop10Service {

  private val hotCategoryAnalysisTop10Dao = new HotCategoryAnalysisTop10Dao()


  // TODO 方案四：累加器
  def analysis4() ={
    //TODO 1.获取用户行为原始数据
    val actionRDD: RDD[String]= hotCategoryAnalysisTop10Dao.textFile()

    // TODO 2.声明累加器并注册
    val acc = new CategoryCountAccumulator
    ProjectUtil.getSparkContext().register(acc)

    //TODO 3.将日志数据进行结构的转换
    actionRDD.foreach(
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

    // TODO 4.获取累加器的值
    val accValue: mutable.Map[String, HotCategory] = acc.value
    val hotCategoryMap = accValue.map(_._2)

    hotCategoryMap.toList.sortWith(
      (left,right) => {
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
  }

  // TODO 方案三：样例类
  def analysis3()={
    //TODO 1.获取用户行为原始数据
    val actionRDD: RDD[String]= hotCategoryAnalysisTop10Dao.textFile()

    //TODO 2.将日志数据进行结构的转换
    val mapRDD :RDD[HotCategory]= actionRDD.flatMap(
      action => {
        //对用户行为类型判断
        val datas = action.split("_")
        // 点击
        if (datas(6) != "-1") {
          List(HotCategory(datas(6), 1, 0, 0))
        } else if (datas(8) != "null") {
          // 下单
          val orderIds = datas(8).split(",")
          orderIds.map(
            id => HotCategory(id, 0, 1, 0))
        } else if (datas(10) != "null") {
          // 支付
          val payIds = datas(10).split(",")
          payIds.map(
            id => HotCategory(id, 0, 0, 1))
        } else {
          Nil
        }
      }
    )
    //将相同的品类分在一起
    val groupRDD: RDD[(String, Iterable[HotCategory])] = mapRDD.groupBy(_.id)

    /*val reduceRDD: RDD[(String, HotCategory)] = groupRDD.mapValues(
      list => list.reduce(
        (c1, c2) => {d
          c1.clickCount = c1.clickCount + c2.clickCount
          c1.orderCount = c1.orderCount + c2.orderCount-kl+
          c1.payCount = c1.payCount + c2.payCount
          c1
        }
      )
    )*/

    val reduceRDD: RDD[HotCategory] = groupRDD.map {
      case (id, list) => {
        list.reduce(
          (c1, c2) => {
            c1.clickCount = c1.clickCount + c2.clickCount
            c1.orderCount = c1.orderCount + c2.orderCount
            c1.payCount = c1.payCount + c2.payCount
            c1
          }
        )
      }
    }

    // sortWith排序
    reduceRDD.collect().sortWith(
      (left,right) => {
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

  }


  // TODO 方案二：减少shuffle次数提高性能
  def analysis2()={

    //TODO 1.获取用户行为原始数据
    val actionRDD: RDD[String]= hotCategoryAnalysisTop10Dao.textFile()

    //TODO 2.将日志数据进行结构的转换
    //String => (String,(int,int,int))
    val mapRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
      action => {
        //对用户行为类型判断
        val datas = action.split("_")
        // 点击
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          // 下单
          val orderIds = datas(8).split(",")
          orderIds.map((_, (0, 1, 0)))
        } else if (datas(10) != "null") {
          // 支付
          val payIds = datas(10).split(",")
          payIds.map((_, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    val reduceRDD: RDD[(String, (Int, Int, Int))] = mapRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    val sortRDD: RDD[(String, (Int, Int, Int))] = reduceRDD.sortBy(_._2,false)

    sortRDD
  }


  // TODO 方案一
  def analysis1() = {

    //TODO 1.获取用户行为原始数据
    val actionRDD: RDD[String]= hotCategoryAnalysisTop10Dao.textFile()

    //TODO 2.根据用户行为类型进行数据统计
    // （品类1，点击量），（品类2，点击量），（品类3，点击量）
    // （品类1，下单数量）,（品类2，下单数量）,（品类3，下单数量）
    // （品类1，支付数量），（品类2，支付数量），（品类3，支付数量）

    //  todo 2.1 统计品类的点击数量（品类1，1）
    val clickRDD: RDD[(String, Int)] = actionRDD.map(
      action => {
        val datas = action.split("_")
        (datas(6), 1)
      }
    ).filter(_._1 != "-1")
    //    todo 对点击数量进行聚合（品类1，sum）
    val categoryIdToClickRDD: RDD[(String, Int)] = clickRDD.reduceByKey(_+_)

    //    todo 将数据格式化方便后续累加
    val value1: RDD[(String, (Int, Int, Int))] = categoryIdToClickRDD.map {
      case (id, num) => {
        (id, (num, 0, 0))
      }
    }

    //  todo 2.2 统计品类的下单数量
    val orderRDD: RDD[String] = actionRDD.map(
      action => {
        val datas = action.split("_")
        datas(8)
      }
    ).filter(_ != "null")
    //    todo 对点击数量进行分解（品类1，1）
    val orderToOneRDD: RDD[(String, Int)] = orderRDD.flatMap(
      orders => {
        val datas = orders.split(",")
        datas.map((_, 1))
      }
    )
    //    todo 对点击数量进行聚合（品类1，sum）
    val categroyIdToOrderRDD: RDD[(String, Int)] = orderToOneRDD.reduceByKey(_+_)
    //    todo 将数据格式化方便后续累加
    val value2: RDD[(String, (Int, Int, Int))] = categroyIdToOrderRDD.map {
      case (id, num) => {
        (id, (0,num, 0))
      }
    }

    //  todo 2.3 统计品类的支付数量
    val payRDD: RDD[String] = actionRDD.map(
      action => {
        val datas = action.split("_")
        datas(10)
      }
    ).filter(_ != "null")
    //    todo 对支付数量进行分解（品类1，1）
    val payToOneRDD: RDD[(String, Int)] = payRDD.flatMap(
      pays => {
        val datas = pays.split(",")
        datas.map((_, 1))
      }
    )
    //    todo 对支付数量进行聚合（品类1，sum）
    val categroyIdToPayRDD: RDD[(String, Int)] = payToOneRDD.reduceByKey(_+_)
    //    todo 将数据格式化方便后续累加
    val value3: RDD[(String, (Int, Int, Int))] = categroyIdToPayRDD.map {
      case (id, num) => {
        (id, (0, 0,num))
      }
    }

    // TODO 相同品类的统计数据聚集在一起
    val reduceRDD = value1.union(value2).union(value3).reduceByKey(
      (t1,t2) => {
        (t1._1+t2._1,t1._2+t2._2,t1._3+t2._3)
      }
    )

    // TODO 排序取前十
    //   tuple的排序默认从第一个开始依次往下
    val resultRDD = reduceRDD.sortBy(_._2,false).take(10)

    resultRDD

  }
}
