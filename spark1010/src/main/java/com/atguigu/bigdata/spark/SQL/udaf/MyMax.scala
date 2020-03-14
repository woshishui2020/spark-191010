package com.atguigu.bigdata.spark.SQL

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}


class MyMax extends UserDefinedAggregateFunction{

  override def inputSchema: StructType =
              StructType(StructField("input",IntegerType)::Nil)

  override def bufferSchema: StructType =
              StructType(StructField("max",IntegerType)::Nil)

  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  // 不给初始值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {}

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    //判断是否为第一次比较
    if (buffer.isNullAt(0)) {
      //判断输入数据是否为null
      if (!input.isNullAt(0)) {
        buffer(0) = input.getInt(0)
      }
    } else {
      //判断输入数据是否为null
      if (!input.isNullAt(0)) {
        buffer(0) = math.max(buffer.getInt(0),input.getInt(0))
      }
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    if (!buffer1.isNullAt(0)) {
      buffer1(0) = math.max(buffer1.getInt(0),buffer2.getInt(0))
    } else {
      buffer1(0) = buffer2.getInt(0)
    }
  }

  override def evaluate(buffer: Row) = buffer.getInt(0)
}

