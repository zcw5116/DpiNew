package com.zyuc.dpi.utils

import com.alibaba.fastjson.JSONObject

/**
  * Created by zhoucw on 下午4:35.
  */
object CommonUtils {
  def getJsonValueByKey(data:JSONObject, key:String) = {
    var info = ""
    val value = data.getString(key)
    if(value == null) {
      info += key + " required not null. "
      throw new JsonValueNotNullException(info)
    }
    value
  }
}
