package com.test

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object test1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val log = sc.textFile("dir/json.txt")
    val logs = log.collect().toBuffer
    var list1:List[String] =List()
    var list2:List[String] = List()

    for(i <- 0 until logs.length) {
      val str: String = logs(i).toString
      val jsonparse = JSON.parseObject(str)
      val status = jsonparse.getIntValue("status")
      if (status == 0) return ""
      val regeocodeJSON = jsonparse.getJSONObject("regeocode")
      if(regeocodeJSON == null || regeocodeJSON.keySet().isEmpty) return ""
      val poisArray = regeocodeJSON.getJSONArray("pois")
      if (poisArray == null || poisArray.isEmpty) return null

      val buffer1 =collection.mutable.ListBuffer[String]()
      val buffer2 = collection.mutable.ListBuffer[String]()
      for(item <- poisArray.toArray){
        if(item.isInstanceOf[JSONObject]){
          val json = item.asInstanceOf[JSONObject]
          buffer1.append(json.getString("businessarea"))
          buffer2.append(json.getString("type"))
        }
      }
      list1 :+= buffer1.mkString(",")
      list2 :+= buffer2.mkString(";")

    }
//    list1.foreach(println)
//    list2.foreach(println)

    val res1 = list1.flatMap(x => x.split(","))
        .filter(x => x!="[]")
        .map(x =>(x,1))
        .groupBy(_._1).mapValues(x => x.size).toList

    val res2 = list2.flatMap(x => x.split(";"))
        .filter(x => x!= "[]")
        .map(x => (x,1))
        .groupBy(_._1).mapValues(x => x.size).toList

    res1.foreach(println)
    println("\n\r")
    res2.foreach(println)

    sc.stop()
  }

}
