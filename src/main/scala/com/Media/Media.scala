package com.Media

import com.Utils.RptUtils
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 取appname和appid,以appname为key，若appname为空则找appid此时需要利用字典文件对appid处理
  * 字典文件：app_dict 需要清洗过滤
  */
object Media {
  def main(args: Array[String]): Unit = {
    if(args.length != 2) {
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    val Array(logDataPath,appDictPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    // 创建执行入口
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    //将app字典文件处理后作为广播变量
    val appMap = spark.sqlContext.read.textFile(appDictPath).filter(_.length > 4).flatMap(line => {
      import scala.collection.mutable.Map
      val map = Map[String,String]()
      val fields: Array[String] = line.split("\t")
      map += (fields(4) -> fields(1))
      map
    }).collect().toMap
    appMap.foreach(println)
    val broadcastAppMap = sc.broadcast(appMap)

    // 获取日志数据
    val df: DataFrame = spark.read.parquet(logDataPath)

    // 将数据进行处理，统计各个指标
    val mapped  = df.map(row=>{
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
      val appname = row.getAs[String]("appname")
      val appid = row.getAs[String]("appid")

      val list1 = RptUtils.request(requestmode,processnode)
      val list2 = RptUtils.click(requestmode,iseffective)
      val list3 = RptUtils.Ad(iseffective,isbilling,isbid,iswin,adorderid,WinPrice,adpayment)

      //判断appname是否为空
      if(appname == null){
        val appName = broadcastAppMap.value.getOrElse(appid, null)
      }
      (appname,list1 ++ list2 ++ list3)
    })
      .rdd.reduceByKey((list1,list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    })
      .map(t => {
        (t._1,t._2.mkString(","))
      })


    sc.stop()
    spark.stop()

  }
}
