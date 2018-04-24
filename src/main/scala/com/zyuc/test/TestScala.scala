package com.zyuc.test

import java.text.SimpleDateFormat
import java.util.Base64

/**
  * Created by zhoucw on 下午3:40.
  */
object TestScala {
  def main(args: Array[String]): Unit = {
    val loadTime = "2018-03-16 16:04:12"
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val endTime = sdf.parse(loadTime)
    val beginUnix = (sdf.parse(loadTime).getTime / 1000).toString
    println(beginUnix)

    val b64Url = Base64.getEncoder.encodeToString("http://d.tclapp.huan.tv/201804151902/6f3fb447588929761edfc20ba83e5bfe/appstore/resources/2014/09/05/cdd8b4947add4de1983caa59e583a20b/TCL/app_1520414707843.apk".getBytes())
    println("b64Url:" + b64Url)

  }
}
