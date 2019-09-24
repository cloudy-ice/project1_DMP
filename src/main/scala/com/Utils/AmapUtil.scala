package com.Utils

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * 商圈解析工具
  */
object AmapUtil {

  // 获取高德地图商圈信息
  def getBusinessFromAmap(long:Double,lat:Double):String={
    //https://restapi.amap.com/v3/geocode
    // /regeo?output=xml&location=116.310003,39.991957&key=<用户的key>&radius=1000&extensions=all
    val location = long+","+lat
    val urlStr = "https://restapi.amap.com/v3/geocode/regeo?key=bfb2e55c6d262f989b0d5521728e3d84&location="+location
    // 调用请求
    val jsonstr = HttpUtil.get(urlStr)
    // 解析json串
    val jsonparse = JSON.parseObject(jsonstr)
    // 判断状态是否成功
    val status = jsonparse.getIntValue("status")
    if(status == 0) return ""
    // 接下来解析内部json串，判断每个key的value都不能为空
    val regeocodeJson = jsonparse.getJSONObject("regeocode")
    if(regeocodeJson == null || regeocodeJson.keySet().isEmpty) return ""

    val addressComponentJson = regeocodeJson.getJSONObject("addressComponent")
    if(addressComponentJson == null || addressComponentJson.keySet().isEmpty) return ""

    val businessAreasArray = addressComponentJson.getJSONArray("businessAreas")
    if(businessAreasArray == null || businessAreasArray.isEmpty) return null
    // 创建集合 保存数据
    val buffer = collection.mutable.ListBuffer[String]()
    // 循环输出
    for(item <- businessAreasArray.toArray){
      if(item.isInstanceOf[JSONObject]){
        val json = item.asInstanceOf[JSONObject]
        buffer.append(json.getString("name"))
      }
    }
    buffer.mkString(",")

  }

  //测试
  def main(args: Array[String]): Unit = {
    val string  = getBusinessFromAmap(116.481488,39.990464)
    print(string)
  }
}
