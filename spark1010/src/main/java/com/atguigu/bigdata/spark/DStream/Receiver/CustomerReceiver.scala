package com.atguigu.bigdata.spark.DStream.Receiver

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class CustomerReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){

  //TODO 自定义数据源，实现监控某个端口号，获取该端口号内容

  //todo 1.2 读数据并将数据发送给Spark
  def receive(): Unit = {

    try {
      //todo 1.3 创建Socket通信
      val socket = new Socket(host, port)

      //todo 1.4 定义变量接收端口传过来的数据
      var input: String = null

      //todo 1.5 创建流，用于读取端口传来的数据
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
      //todo 1.6 读取数据
      input = reader.readLine()

      //todo 1.7 当receiver没有关闭并且输入数据不为空，则循环发送数据给Spark
      while (input != null && !isStopped()) {
        //todo 1.8 写入Spark内存
        store(input)
        //todo 1.9
        input = reader.readLine()
      }

      //todo 出现异常
      reader.close()
      socket.close()
      restart("重启")
    } catch {
      case e:Exception => restart("重启")

    }
  }

  //todo 1.接收器开启时调用的方法
  override def onStart(): Unit = {
    //todo 1.1开启新的线程接收数据
    new Thread() {
      override def run(): Unit = receive()
    }.start()
  }

  //todo 2.接收器开启时调用的方法
  override def onStop(): Unit = {}
}
