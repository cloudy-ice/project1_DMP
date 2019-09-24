package com.Tags

import com.Utils.Tag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsKeyWord extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val stopword = args(1).asInstanceOf[Broadcast[String]]
    val kwds = row.getAs[String]("keywords").split("\\|")
    //过滤停用词
    kwds.filter(word =>{
      word.length>=3 && word.length <= 8 && !stopword.value.contains(word)
    })
        .foreach(word => list:+=("K"+word,1))
    list
  }
}
