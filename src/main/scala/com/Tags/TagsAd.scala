package com.Tags

import com.Utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 广告标签
  * 渠道标签
  */
object TagsAd extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]
    val adType = row.getAs[Int]("adspacetype")
    adType match {
      case v if v > 9 => list:+=("LC"+v,1)
      case v if v <= 9 && v > 0 => list:+=("LC0"+v,1)
    }
    val adName = row.getAs[String]("adspacetypename")
    if(StringUtils.isNotBlank(adName)){
      list:+=("LN"+adName,1)
    }

    // 渠道标签
    val channel = row.getAs[Int]("adplatformproviderid")
    list:+=("CN"+channel,1)
    list
  }
}
