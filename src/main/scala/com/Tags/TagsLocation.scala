package com.Tags

import com.Utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsLocation extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args.asInstanceOf[Row]
    val province = row.getAs[String]("rtbprovince")
    val city = row.getAs[String]("rtbcity")

    if(StringUtils.isNotBlank(province)){
      list:+=("ZP"+province,1)
    }
    if(StringUtils.isNotBlank(city)){
      list:+=("ZC"+city,1)
    }
    list
  }
}
