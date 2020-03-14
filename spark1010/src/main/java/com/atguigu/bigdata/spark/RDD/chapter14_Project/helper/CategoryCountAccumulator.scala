package com.atguigu.bigdata.spark.RDD.chapter14_Project.helper


import com.atguigu.bigdata.spark.RDD.chapter14_Project.bean.HotCategory
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class CategoryCountAccumulator extends AccumulatorV2[(String,String),mutable.Map[String, HotCategory]]{

  val map = mutable.Map[String, HotCategory]()

  override def isZero: Boolean = map.isEmpty


  override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
    new CategoryCountAccumulator
  }

  override def reset(): Unit = map.clear()


  override def add(v: (String, String)): Unit = {
    val hotCategory = map.getOrElse(v._1,HotCategory(v._1,0,0,0))
    v._2 match {
      case "click" => hotCategory.clickCount += 1
      case "order" => hotCategory.orderCount += 1
      case "pay" => hotCategory.payCount += 1
      case _ =>
    }
    map(v._1) = hotCategory
  }

  override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]])= {

    other.value.foreach{
      case (cid,hotCategory) => {
        val hc = map.getOrElse(cid,HotCategory(cid,0,0,0))
        hc.clickCount += hotCategory.clickCount
        hc.orderCount += hotCategory.orderCount
        hc.payCount += hotCategory.payCount

        map(cid) = hc
      }
    }
  }

  override def value: mutable.Map[String, HotCategory] = map

}
