package com.zyuc.test

import java.text.SimpleDateFormat

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

  }
}
