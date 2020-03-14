package com.atguigu.bigdata.spark.SQL.hive

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}

class MyUDAF extends UserDefinedAggregateFunction{

  override def inputSchema: StructType =
    StructType(StructField("input",StringType)::Nil)

  // Map(北京->100,天津->50...),500
  override def bufferSchema: StructType = StructType(StructField("city_count",
    MapType(StringType,LongType))::StructField("total",LongType)::Nil)

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String,Long]()
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    // 获取输入数据的城市名称
    val cityName: String = input.getString(0)

    // 获取缓存数据
    val cityCount: collection.Map[String, Long] = buffer.getMap[String,Long](0)

    // 根据缓存数据是否有该城市名称，做不同处理
    buffer(0) = cityCount + (cityName -> (cityCount.getOrElse(cityName,0L) + 1L))

    // 总数自增
    buffer(1) = buffer.getLong(1) + 1L
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    val map1: collection.Map[String, Long] = buffer1.getMap[String,Long](0)
    val map2: collection.Map[String, Long] = buffer2.getMap[String,Long](0)
    val total1:Long = buffer1.getLong(1)
    val total2:Long = buffer2.getLong(1)

    buffer1(0) = map1.foldLeft(map2){
      case (map,(cityName,count)) => {
        map + (cityName -> (map.getOrElse(cityName,0L) + count))
      }
    }

    buffer1(1) = total1 + total2
  }

  override def evaluate(buffer: Row) = {

    // 获取数据
    val cityCount: collection.Map[String, Long] = buffer.getMap[String,Long](0)
    val total: Long = buffer.getLong(1)

    // 排序
    val top2CityCount: List[(String, Long)] = cityCount.toList.sortWith(_._2 > _._2).take(2)

    var otherRatio = 1D

    // 计算占比
    var ratios: List[CityRatio] = top2CityCount.map {
      case (cityName, count) =>
        // 计算前两名城市占比
        val cityRatio: Double = count.toDouble / total
        otherRatio -= cityRatio

        CityRatio(cityName, cityRatio)
    }

    // 添加“其他”
    if (cityCount.size > 2) {
      ratios = ratios :+ CityRatio("其他", otherRatio)

    }

      ratios.mkString(",")

  }
}
