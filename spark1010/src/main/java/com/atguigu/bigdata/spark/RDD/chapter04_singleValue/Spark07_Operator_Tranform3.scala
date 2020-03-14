package com.atguigu.bigdata.spark.RDD.chapter04_singleValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_Operator_Tranform3 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val list : List[Int] = List(4,1,2,3,2,3,4)
    val listRDD3: RDD[Int] = sc.makeRDD(list,2)

    // TODO  算子 转换 单Value类型

    // TODO: 1. distinct 去重
    val distinctRDD: List[Int] = list.distinct
    //distinctRDD.foreach(println)

    // TODO: 2. sample采样
    //   true:有放回的抽样,可重复; false为无放回的抽样,不可重复
    //   fraction：以指定的随机种子随机抽样出数量为fraction的数据
    //   seed:种子，可选的，一旦有了种子后续的结果就不会变了
    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6))
    val sampleRDD: RDD[Int] = listRDD.sample(true,0.5,10)
    //println(sampleRDD.collect().mkString(","))

    // TODO: 3. coalesce()合并分区
    //   默认的分区会导致数据分配不均匀，数据倾斜
    //   shuffle(可选)：可以在合并数据时将数据打散重新组合合并分区
    //   如果少分区想变成多分区则必须使用shuffle
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 4)
    println(rdd.getNumPartitions)
    val coalesceRDD: RDD[Int] = rdd.coalesce(2)
    val indexRDD: RDD[(Int, Int)] = coalesceRDD.mapPartitionsWithIndex(
      (index, list) =>
        list.map((index,_))
    )
    println(indexRDD.collect().mkString(","))
    // todo:执行shuffle(会将数据打散)
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6),4)
    val coalesceRDD1: RDD[Int] = rdd1.coalesce(2,true)
    val indexRDD1: RDD[(Int, Int)] = coalesceRDD1.mapPartitionsWithIndex(
      (index, list) =>
        list.map((index,_))
    )
    println(indexRDD1.collect().mkString(","))

    // TODO: 4. repartition重新分区
    //            （底层就是coalesce的shuffle操作）
    val rdd2: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)
    val repartitionRDD: RDD[Int] = rdd2.repartition(2)
    repartitionRDD.mapPartitionsWithIndex(
      (index, list) =>
        list.map((index, _))
    ).collect().mkString(",")

    // TODO: 5. coalesce和repartition的区别
    //   coalesce重新分区，可以选择是否进行shuffle过程
    //   coalesce一般为缩减分区，如果扩大分区，也不会增加分区总数，意义不大。
    //   repartition扩大分区执行shuffle，可以达到扩大分区的效果

    // TODO: 6. sortBy()排序
    val rdd3: RDD[Int] = sc.makeRDD(List(3,2,5,4,1))
    val sortRDD: RDD[Int] = rdd3.sortBy(num => num)
    println(sortRDD.collect().mkString(","))

    val strRDD: RDD[String] = sc.makeRDD(List("1", "22", "12", "2", "3"))
    println(strRDD.sortBy(s => s.toInt).collect().mkString(","))

    sc.stop()
  }
}
