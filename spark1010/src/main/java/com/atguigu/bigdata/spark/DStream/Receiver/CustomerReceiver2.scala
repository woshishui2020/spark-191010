package com.atguigu.bigdata.spark.DStream.Receiver

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver


class CustomerReceiver2(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){

  def receive(): Unit = {

    try {
      val socket = new Socket(host, port)

      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))

      var input: String = reader.readLine()

      while (input != null && !isStopped()) {
        store(input)
        input = reader.readLine()
      }

      reader.close()
      socket.close()

      restart("重启")

    } catch {
      case e: Exception => restart("restart")
    }
  }

  override def onStart(): Unit = {
    new Thread(){
      override def run(): Unit = receive()
    }.start()
  }

  override def onStop(): Unit = {}
}
