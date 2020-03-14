package com.atguigu.bigdata.spark.DStream.project.utils

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource

object JdbcUtil {

  //初始化连接池
  var dataSource: DataSource = init()

  //初始化连接池方法
  def init(): DataSource = {
    val properties = new Properties()
    val config: Properties = PropertiesUtil.load("config.properties")
    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", config.getProperty("jdbc.url"))
    properties.setProperty("username", config.getProperty("jdbc.user"))
    properties.setProperty("password", config.getProperty("jdbc.password"))
    properties.setProperty("maxActive", config.getProperty("jdbc.datasource.size"))
    DruidDataSourceFactory.createDataSource(properties)

  }
  //todo 获取MySQL连接
  def getConnection: Connection = {
    dataSource.getConnection
  }

    //todo 判断某条数据是否存在
    def isExsit(connection: Connection, sql: String, params: Array[Any]): Boolean = {

      var flag = false
      var preparedStatement: PreparedStatement = null

      try {
        //预编译sql
        preparedStatement = connection.prepareStatement(sql)

        //占位符赋值
        for (elem <- 1 to params.length) {
          preparedStatement.setObject(elem,params(elem))
        }

        //执行查询
        flag = preparedStatement.executeQuery().next()
        preparedStatement.close()

      } catch {
        case e: Exception => e.printStackTrace()
      }
      flag
    }

  //todo 插入一条数据
  def executeUpdate(connection: Connection, sql: String, params: Array[Any]): Unit = {

    var preparedStatement: PreparedStatement = null
    try {
      //预编译sql
      preparedStatement = connection.prepareStatement(sql)

      //占位符赋值并查询
      for (elem <- 1 to params.length) {
        preparedStatement.setObject(elem,params(elem))
      }
      preparedStatement.executeUpdate()
      preparedStatement.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  //todo 获取数据
  def getData(connection: Connection, sql: String, params: Array[Any]): Long ={

    var result: Long = 0L
    var preparedStatement: PreparedStatement = null
    try {
      //预编译sql
      preparedStatement = connection.prepareStatement(sql)

      //占位符赋值并查询获取数据
      for (elem <- 1 to params.length) {
        preparedStatement.setObject(elem,params(elem))
      }
      val resultSet: ResultSet = preparedStatement.executeQuery()
      if (resultSet.next()) {
        result = resultSet.getLong(1)
      }
      resultSet.close()
      preparedStatement.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    result
  }


  def main(args: Array[String]): Unit = {

    val connection: Connection = getConnection

    println(isExsit(connection, "select * from black_list where userid =?", Array("1")))
    println(isExsit(connection, "select * from black_list where userid =?", Array("2")))

    connection.close()
  }
}
