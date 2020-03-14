package com.atguigu.bigdata.spark.DStream.project.handler

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bigdata.spark.DStream.project.bean.Ads_log
import com.atguigu.bigdata.spark.DStream.project.utils.JdbcUtil
import org.apache.spark.streaming.dstream.DStream

object BlackListHandler {

  //时间格式对象
  private val format = new SimpleDateFormat("yyyy-MM--dd")

  //TODO 统计用户数据，将达到100次的用户加入黑明单
  def saveBlackListToMysql(filterAdsLogDStream: DStream[Ads_log])= {

    //todo 1.转换数据结构 ads_log => ((dt,userid,adid),1)
    val dtUserAdToOneDStream: DStream[((String, String, String), Int)] = filterAdsLogDStream.map {
      ads_log => {
        val timestamp: Long = ads_log.timestamp
        val dt: String = format.format(new Date(timestamp))
        ((dt, ads_log.userid, ads_log.adid), 1)
      }
    }

    //todo 2.统计总次数
    val dtUserAdToSumDStream: DStream[((String, String, String), Int)] = dtUserAdToOneDStream.reduceByKey(_+_)

    //todo 3.将当前批次数据写库，并和100作比较是否写入黑明单
    dtUserAdToSumDStream.foreachRDD(
      rdd => {

        //todo 3.1减少创建连接，对每个分区的数据写库，foreachPartition
        rdd.foreachPartition(
          item => {

            //todo 3.2获取连接
            val connection: Connection = JdbcUtil.getConnection

            //todo 3.3写库操作
            item.foreach{
              case ((dt,userId,adId),count) => {

                //todo a.将当前批次数据结合MySQL中已有数据更新
                JdbcUtil.executeUpdate(
                  connection,
                  """
                    |insert into user_ad_count(dt,userId,adId,count)
                    |values(?,?,?,?)
                    |on duplicate key
                    |update count = count + ?
                  """.stripMargin,
                  Array(dt,userId,adId,count,count))

                //todo b.获取MySQL中的点击次数，判断是否 > 100
                val result: Long = JdbcUtil.getData(
                  connection,
                  """
                    |select
                    |    count
                    |from
                    |    user_ad_count
                    |where
                    |    dt = ? and userId = ? and adId = ?
                  """.stripMargin,
                  Array(dt,userId,adId))

                //todo c. > 100 写入黑名单
                if (result >= 100) {
                  JdbcUtil.executeUpdate(
                    connection,
                    """
                      |insert into black_list(userId)
                      |values(?)
                      |on duplicate key
                      |update userId = ?
                    """.stripMargin,
                    Array(userId,userId))
                }
              }
            }

            //todo 3.4释放连接
            connection.close()

          }
        )
      }
    )
  }
}
