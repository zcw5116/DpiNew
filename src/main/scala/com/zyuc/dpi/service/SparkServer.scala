package com.zyuc.dpi.service

/**
  * Created by zhoucw on 17-7-17.
  */

import java.io.OutputStream
import java.net.InetSocketAddress
import java.util.concurrent.Executors
import com.alibaba.fastjson.JSON
import com.sun.net.httpserver.{Headers, HttpExchange, HttpHandler, HttpServer}
import com.typesafe.config.Config
import com.zyuc.dpi.utils.ConfigUtil
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

object SparkServer {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().enableHiveSupport().master("local[*]").appName("test").getOrCreate()
    val sc = spark.sparkContext
    val port = sc.getConf.get("spark.server.port", "9999")
    // 配置文件absolutepath
    val configFile = sc.getConf.get("spark.app.config", "#")
    val fileSystem = FileSystem.get(sc.hadoopConfiguration)
    val config = if (configFile == "#") ConfigUtil.getConfig else ConfigUtil.getConfig(configFile)

    createServer(port.toInt, spark, fileSystem, config)

  }

  def createServer(port: Int, spark: SparkSession, fileSystem: FileSystem, config: Config): Unit = {

    val httpServer = HttpServer.create(new InetSocketAddress(port), 30)
    httpServer.setExecutor(Executors.newCachedThreadPool())
    httpServer.createContext("/convert", new HttpHandler() {
      override def handle(httpExchange: HttpExchange): Unit = {
        System.out.println("处理新请求:" + httpExchange.getRequestMethod + " , " + httpExchange.getRequestURI)
        var response = "正常"
        var httpCode = 200
        val requestHeaders = httpExchange.getRequestHeaders
        val contentLength = requestHeaders.getFirst("Content-length").toInt
        System.out.println("" + requestHeaders.getFirst("Content-length"))
        val inputStream = httpExchange.getRequestBody
        val data = new Array[Byte](contentLength)
        val length = inputStream.read(data)
        System.out.println("data:" + new String(data))
        val params = JSON.parseObject(new String(data))
        val responseHeaders: Headers = httpExchange.getResponseHeaders
        responseHeaders.set("Content-Type", "text/html;charset=utf-8")

        var serverInfo = ""

        try {
          serverInfo = ServerLine.access(spark, fileSystem, config, params)
        } catch {
          case e: Exception =>
            e.printStackTrace()
            response = "失败"
            httpCode = 500
        }
        println("serverInfo:" + serverInfo)
        response = "HttpServerStatus: " + response + "" + serverInfo

        httpExchange.sendResponseHeaders(httpCode, response.getBytes.length)
        val responseBody: OutputStream = httpExchange.getResponseBody
        responseBody.write(response.getBytes)
        responseBody.flush
        responseBody.close
      }
    })

    httpServer.createContext("/ping", new HttpHandler() {
      override def handle(httpExchange: HttpExchange): Unit = {
        var response = "存活"
        var httpCode = 200

        try {
          if (spark.sparkContext.isStopped) {
            httpCode = 400
            response = "spark终止"
          }
        } catch {
          case e: Exception =>
            httpCode = 500
            response = "服务异常"
        } finally {
          httpExchange.sendResponseHeaders(httpCode, response.getBytes().length)
          val out = httpExchange.getResponseBody //获得输出流
          out.write(response.getBytes())
          out.flush()
          httpExchange.close()
        }
      }
    })

    /**
      * 停止sc测试
      */
    httpServer.createContext("/stop_sc", new HttpHandler() {
      override def handle(httpExchange: HttpExchange): Unit = {
        var response = "成功"
        var httpCode = 200

        try {
          spark.sparkContext.stop()
        } catch {
          case e: Exception =>
            httpCode = 500
            response = "服务异常"
        } finally {
          httpExchange.sendResponseHeaders(httpCode, response.getBytes().length)
          val out = httpExchange.getResponseBody //获得输出流

          out.write(response.getBytes())
          out.flush()
          httpExchange.close()
        }
      }
    })

    httpServer.start()
    println("SparkServer started " + port + " ......")
  }
}