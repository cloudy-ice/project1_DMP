package com.Utils

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

/**
  * http请求协议
  */
object HttpUtil {
  //GET请求
  def get(url:String):String = {
    val client = HttpClients.createDefault()
    val get = new HttpGet(url)
    //发送请求
    val response = client.execute(get)
    //获取返回结果
    EntityUtils.toString(response.getEntity,"UTF-8")
  }
}
