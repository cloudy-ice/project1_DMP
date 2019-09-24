package com.Tags

import com.Utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row


object TagsApp extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val appmap = args(1).asInstanceOf[Broadcast[Map[String,String]]]
    //获取appid和appname
    val appname = row.getAs[String]("appname")
    val appid = row.getAs[String]("appid")
    //空值判断
    if(StringUtils.isNotBlank(appname)){
      list:+=("APP"+appname,1)
    }else if(StringUtils.isNoneBlank(appid)){
      list:+=("APP"+appmap.value.getOrElse(appid,appid),1)
    }
    list
  }
}
