package com.atguigu.bigdata.spark.DStream.project.app

import java.sql.Connection

import com.atguigu.bigdata.spark.DStream.project.bean.Ads_log
import com.atguigu.bigdata.spark.DStream.project.handler.{BlackListHandler, DateAreaCityAdCountHandler, LastHourAdCountHandler}
import com.atguigu.bigdata.spark.DStream.project.utils.{JdbcUtil, MyKafkaUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealTimeApp {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("RealTimeApp").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(3))

    //todo 1.读取kafka数据 (1583313642306 华南 深圳 2 1)
    val topic: String = PropertiesUtil.load("config.properties")
      .getProperty("kafka.topic")
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic,ssc)

    //todo 2.将数据转换为样例类对象
    val adsLogDStream: DStream[Ads_log] = kafkaDStream.map(
      line => {
        val arr: Array[String] = line.value().split(" ")
        //todo 封装为样例类对象
        Ads_log(arr(0).toLong, arr(1), arr(2), arr(3), arr(4))
      })

    //todo 3.根据MySQL中的黑明单表进行数据过滤
    val filterAdsLogDStream: DStream[Ads_log] = adsLogDStream.filter(
      adsLog => {
        //todo 查询当前用户是否存在
        val connection: Connection = JdbcUtil.getConnection
        val bool: Boolean = JdbcUtil.isExsit(connection, "select * from black_list where userid=?", Array(adsLog.userid))

        !bool
      })

    filterAdsLogDStream.cache()

    //todo 4.将未加入黑名单的用户数据更新到MySQL
    BlackListHandler.saveBlackListToMysql(filterAdsLogDStream)

    //todo 5.统计每天各大区各个城市广告点击数
    DateAreaCityAdCountHandler.saveDateAreaCityAdCountToMysql(filterAdsLogDStream)

    //todo 6.统计最近（1小时）2分钟广告分时点击总数
    val adToHmCountListDStream: DStream[(String, List[(String, Long)])] = LastHourAdCountHandler.getAdHourMinToCount(filterAdsLogDStream)


    adToHmCountListDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
