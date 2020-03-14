package com.atguigu.bigdata.spark.SQL

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StructField, StructType}

class MyAvg extends UserDefinedAggregateFunction{

  // todo 定义输入数据类型
  override def inputSchema: StructType = StructType(StructField("input",IntegerType)::Nil)

  // todo 定义中间数据类型
  override def bufferSchema: StructType = StructType(StructField("sum",IntegerType)::StructField("total",IntegerType)::Nil)

  // todo 定义输出数据类型
  override def dataType: DataType = DoubleType

  // todo 函数稳定性
  override def deterministic: Boolean = true

  // todo 中间缓存赋出事值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
    buffer(1) = 0
  }

  // todo 分区内更新缓存的数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getInt(0) + input.getInt(0)
    buffer(1) = buffer.getInt(1) +1

  }

  // todo 分区间数据累加
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0)
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }

  // todo 计算最终结果
  override def evaluate(buffer: Row): Double = {
    buffer.getInt(0).toDouble / buffer.getInt(1)

  }
}

























