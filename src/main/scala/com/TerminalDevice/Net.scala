package com.TerminalDevice

import com.Utils.RptUtils
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Net {
  def main(args: Array[String]): Unit = {
    //判断路径是否正确
    if (args != 1) {
      println("目录参数不正确")
      sys.exit(0)
    }
    //创建一个集合保存目录
    val Array(inputPath) = args
    //初始化
    val conf = new SparkConf().setAppName("this.getClass.getName").setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val df = spark.read.parquet(inputPath)

    val mapped = df.map(row => {
      // 把需要的字段全部取到
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val WinPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      //  判断appname,appname做key
      val networkmannername = row.getAs[String]("networkmannername")

      val list1 = RptUtils.request(requestmode, processnode)
      val list2 = RptUtils.click(requestmode, iseffective)
      val list3 = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)

      (networkmannername,(list1 ++ list2 ++ list3))
    })
      .rdd.reduceByKey((list1,list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    })
      .map(t => {
        (t._1,t._2.mkString(","))
      })

    spark.stop()
  }

}
