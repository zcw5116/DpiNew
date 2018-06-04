package com.zyuc.test

import java.text.SimpleDateFormat
import java.util.Base64

import org.apache.spark.sql.functions.udf

import scala.util.matching.Regex

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

    var b64Url = Base64.getEncoder.encodeToString("http://kk.90wd.cn:883/data/kj83_com.js?_=1509796872939".getBytes())
    println("b64Url:" + b64Url)

    b64Url = Base64.getEncoder.encodeToString("".getBytes())
    b64Url = "aHR0cDovL2trLjkwd2QuY246ODgzL2RhdGEva2o4M19jb20uanM/Xz0xNTA5Nzk2ODcyOTM5"
    val url = Base64.getDecoder.decode(b64Url)
    println("url:" + new String(url))

    val surl = "gspe19-cn.ls.apple.com/tiles?style=20&size=2&scale=0&v=249&z=10&x=852&y=420&checksum=1&sid=0997243714311162538006649450721943224335&accessKey=1527563538_ctniZII8owDMjRPd_Peh0RImEtU2oinBuECHdkLTYfevzD2AQIUZcUmhegvKslK7tzGWFj%2FNlN%2BvAXAsbXwa%2BVxN55D%2B%2F3c%2FuwaIAdyG8DBXVGUg%2Fcx2biL5Aw4%2FrVLw5W36o%2FwLIuVgdMUeyi5qdSAMWG0SSdZO6BkSwqnDFIdvomQfm7iHBlZMd6fGzbLLcIp1HB%2B%2Fu"
    b64Url = Base64.getEncoder.encodeToString(surl.getBytes())

    println("b64Url:" + b64Url)


    val partitionNum = 1
    val preHourPartNum = if (partitionNum / 3 == 0) 1 else partitionNum / 3
    println("preHourPartNum:" + preHourPartNum)

    val filesize = 12l
    val msize = filesize.asInstanceOf[Double] / 1024 / 1024 / 128
    val t = Math.ceil(msize).toInt
    println("t:" + t)
    println("msize:" + msize)

  }
}
