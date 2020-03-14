package com.atguigu.bigdata.spark.DStream.project.handler

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bigdata.spark.DStream.project.bean.Ads_log
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream


object LastHourAdCountHandler {

  private val format = new SimpleDateFormat("HH:mm")

  // TODO 统计最近（1小时）2分钟广告分时点击总数
  def getAdHourMinToCount(filterAdsLogDStream: DStream[Ads_log]): DStream[(String, List[(String, Long)])] = {

    //todo 1.开窗
    val windowDStream: DStream[Ads_log] = filterAdsLogDStream.window(Minutes(2))

    //todo 2.转换结构 ads_log => ((adid,hourmin),1L)
    val adHmToOneDStream: DStream[((String, String), Long)] = windowDStream.map(
      ads_log => {
        val timestamp: Long = ads_log.timestamp
        val hourmin: String = format.format(new Date(timestamp))

        ((ads_log.adid, hourmin), 1L)
      })

    //todo 3.统计总数
    val adHmToSumDStream: DStream[((String, String), Long)] = adHmToOneDStream.reduceByKey(_+_)

    //todo 4.转换结构 ((adid,hourmin),sum) => (adid,(hourmin,sum))
    val adHmToCountDStream: DStream[(String, (String, Long))] = adHmToOneDStream.map{
        case ((adid, hourmin), sum) => {
          (adid, (hourmin, sum))
        }
    }

    //todo 5.分组并排序
    adHmToCountDStream.groupByKey().mapValues(
      item => {
        item.toList.sortWith(_._1 < _._1)
      }
    )

  }

}
