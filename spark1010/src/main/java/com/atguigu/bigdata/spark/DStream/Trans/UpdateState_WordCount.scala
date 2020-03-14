package com.atguigu.bigdata.spark.DStream.Trans

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateState_WordCount {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wc")
    val ssc = new StreamingContext(conf,Seconds(3))

    ssc.checkpoint("ck2")

    //todo 1. 端口获取DStream流数据
    val dStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",8888)

    //todo 2. 转换类型
    val wordToOneDStream: DStream[(String, Int)] = dStream.flatMap(_.split(" ")).map((_,1))

    //todo 3. UpdateState_WordCount
    val updateFun: (Seq[Int], Option[Int]) => Some[Int] = (seq:Seq[Int], state:Option[Int]) => {
      //todo 3.1 当前批次
      val sum: Int = seq.sum
      //todo 3.2 之前的计算结果
      val lastSum: Int = state.getOrElse(0)
      //todo 3.3 新的状态
      Some(sum + lastSum)}

    val wordCountDStream: DStream[(String, Int)] = wordToOneDStream.updateStateByKey(updateFun)

    wordCountDStream.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
