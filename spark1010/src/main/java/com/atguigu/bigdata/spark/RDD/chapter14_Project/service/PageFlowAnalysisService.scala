package com.atguigu.bigdata.spark.RDD.chapter14_Project.service

import com.atguigu.bigdata.spark.RDD.chapter14_Project.bean
import com.atguigu.bigdata.spark.RDD.chapter14_Project.common.{AbstractService, TDao}
import com.atguigu.bigdata.spark.RDD.chapter14_Project.dao.PageFlowAnalysisDao
import org.apache.spark.rdd.RDD


class PageFlowAnalysisService extends AbstractService{

  private val pageFlowAnalysisDao = new PageFlowAnalysisDao

  override def getDao(): TDao = pageFlowAnalysisDao

  def analysis2() = {
    val actionRDD: RDD[bean.UserVisitAction] = getUserVisitActions()

    // todo 定义关心的页面
    val flowIds = List(1, 2, 3, 4, 5, 6, 7)
    val zipFlowIds: List[String] = flowIds.zip(flowIds.tail).map {
      case (pageId1, pageId2) => {
        pageId1 + "-" + pageId2
      }
    }



    // todo 计算分母
    // todo 数据过滤
    val filterRDD: RDD[bean.UserVisitAction] = actionRDD.filter(action => flowIds.init.contains(action.page_id))

    val pageToOneRDD: RDD[(Long, Long)] = actionRDD.map(action => {(action.page_id,1L)})
    val pageToSumRDD: RDD[(Long, Long)] = pageToOneRDD.reduceByKey(_+_)
    val pageCount: Array[(Long, Long)] = pageToSumRDD.collect()

    // todo 计算分子
    // todo 根据session进行分组
    val groupRDD: RDD[(String, Iterable[bean.UserVisitAction])] = actionRDD.groupBy(_.session_id)

    // todo 排序（降序）
    val sessionRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      datas => {
        val actions: List[bean.UserVisitAction] = datas.toList.sortWith(
          _.action_time < _.action_time)
        // todo 结构转换
        val pageids: List[Long] = actions.map(_.page_id)
        // todo 将连续的页面进行关联
        val zips: List[(Long, Long)] = pageids.zip(pageids.tail)
        val flowToOne: List[(String, Int)] = zips.map {
          case (pageid1, pageid2) => {
            (pageid1 + "-" + pageid2, 1)
          }
        }
        // 数据过滤
        flowToOne.filter{
          case (ids,one) => {
          zipFlowIds.contains(ids)
          }
        }
      }
    )
    // todo 结构转换
    val pageFlowToOneRDD: RDD[(String, Int)] = sessionRDD.map(_._2).flatMap(list => list)
    val pageFlowToSumRDD: RDD[(String, Int)] = pageFlowToOneRDD.reduceByKey(_+_)

    pageFlowToSumRDD.foreach{
      case (pagefolw,sum) => {
        // 获取分母的数据
        val pageCountMap: Map[Long, Long] = pageCount.toMap
        val pid: String = pagefolw.split("-")(0)
        val value: Long = pageCountMap.getOrElse(pid.toLong,1L)
        println(pagefolw + "=" + sum.toDouble / value)
      }
    }
  }



  def analysis1() = {

    val actionRDD: RDD[bean.UserVisitAction] = getUserVisitActions()

    // todo 计算分母
    val pageToOneRDD: RDD[(Long, Long)] = actionRDD.map(action => {(action.page_id,1L)})
    val pageToSumRDD: RDD[(Long, Long)] = pageToOneRDD.reduceByKey(_+_)
    val pageCount: Array[(Long, Long)] = pageToSumRDD.collect()

    // todo 计算分子
    // todo 根据session进行分组
    val groupRDD: RDD[(String, Iterable[bean.UserVisitAction])] = actionRDD.groupBy(_.session_id)

    // todo 排序，结构转换
    val sessionRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      datas => {
        val actions: List[bean.UserVisitAction] = datas.toList.sortWith(
          _.action_time < _.action_time)
        // todo 结构转换
        val pageids: List[Long] = actions.map(_.page_id)
        // todo 将连续的页面进行关联
        val zips: List[(Long, Long)] = pageids.zip(pageids.tail)
        zips.map {
          case (pageid1, pageid2) => {
            (pageid1 + "-" + pageid2, 1)
          }
        }
      }
    )

    // todo 结构转换
    val pageFlowToOneRDD: RDD[(String, Int)] = sessionRDD.map(_._2).flatMap(list => list)
    val pageFlowToSumRDD: RDD[(String, Int)] = pageFlowToOneRDD.reduceByKey(_+_)

    pageFlowToSumRDD.foreach{
      case (pagefolw,sum) => {
        // 获取分母的数据
        val pageCountMap: Map[Long, Long] = pageCount.toMap
        val pid: String = pagefolw.split("-")(0)
        val value: Long = pageCountMap.getOrElse(pid.toLong,1L)
        println(pagefolw + " = " + (sum.toDouble / value))
      }
    }
  }




  def analysis3() ={

    val actionRDD: RDD[bean.UserVisitAction] = getUserVisitActions()

    val fenmu: RDD[(Long, Long)] = actionRDD.map(action => {(action.page_id,1L)}).reduceByKey(_+_)
    val tuples1: Array[(Long, Long)] = fenmu.collect()

    val groupRDD: RDD[(String, Iterable[bean.UserVisitAction])] = actionRDD.groupBy(_.session_id)

    val sessionRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      action => {
        val sortAction: List[bean.UserVisitAction] = action.toList.sortWith(_.action_time < _.action_time)
        val longs: List[Long] = sortAction.map(_.page_id)

        val tuples: List[(Long, Long)] = longs.zip(longs.tail)
        tuples.map {
          case (p1, p2) => {
            (p1 + "_" + p2, 1)
          }
        }
      }
    )


    val value: RDD[List[(String, Int)]] = sessionRDD.map(_._2)
    val value1: RDD[(String, Int)] = value.flatMap(list => list)

    val value3: RDD[(String, Int)] = value1.reduceByKey(_+_)

    value3.foreach{
      case (page_page,sum) => {
        val str: String = page_page.split("_")(0)
        val l: Long = tuples1.toMap.getOrElse(str.toLong,1L)

        println(sum.toDouble / l)
      }
    }
  }


  // todo 统计网站页面的平均停留时间

  def analysis4() = {

    val actionRDD: RDD[bean.UserVisitAction] = getUserVisitActions()

    val groupRDD: RDD[(String, Iterable[bean.UserVisitAction])] = actionRDD.groupBy(action => action.session_id)

    groupRDD.mapValues(
      action => {
        val actions: List[bean.UserVisitAction] = action.toList.sortWith(_.action_time < _.action_time)
        val tuples: List[(Long, Long)] = actions.map(action => (action.page_id,action.action_time.toLong))

      }
    )
  }
}
