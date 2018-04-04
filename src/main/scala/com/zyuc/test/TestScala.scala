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
    val beginTime = sdf.format(endTime.getTime() - 2*60*60*1000)
    println(beginTime)
    val curHour = loadTime.substring(11,13)
    val preHour = beginTime.substring(11,13)
    println(curHour)
    println(preHour)
  }
}
