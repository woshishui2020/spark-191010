package com.atguigu.bigdata.spark.DStream.project.handler

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bigdata.spark.DStream.project.bean.Ads_log
import com.atguigu.bigdata.spark.DStream.project.utils.JdbcUtil
import org.apache.spark.streaming.dstream.DStream

object DateAreaCityAdCountHandler {

  private val format = new SimpleDateFormat("yyyy-MM--dd")

  //todo 统计各大区各城市广告点击总数
  def saveDateAreaCityAdCountToMysql(filterAdsLogDStream: DStream[Ads_log]): Unit = {

    val dateAreaCityAdToCount: DStream[((String, String, String,String), Long)] = filterAdsLogDStream.map(
      ads_log => {
        val timestamp: Long = ads_log.timestamp
        val dt: String = format.format(new Date(timestamp))
        ((dt, ads_log.area, ads_log.city,ads_log.adid), 1L)
      }
    ).reduceByKey(_ + _)

    dateAreaCityAdToCount.foreachRDD(
      rdd =>
        rdd.foreachPartition(
          item => {
            val connection: Connection = JdbcUtil.getConnection
            item.foreach{
              case ((dt,area,city,adId),count) => {
                JdbcUtil.executeUpdate(
                  connection,
                  """
                    |insert into area_city_ad_count(dt,area,city,adId,count)
                    |values(?,?,?,?,?)
                    |on duplicate key
                    |update count = count + ?
                  """.stripMargin,
                  Array(dt,area,city,adId,count,count))
              }
            }
            connection.close()
          }
        )
    )
  }
}
