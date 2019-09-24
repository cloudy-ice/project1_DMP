package com.ProCityCt

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 3.2.0统计各省市数据量分布情况
  * 要求一：将统计的结果输出成 json 格式，并输出到磁盘目录。
  * 要求二：将结果写到到 mysql 数据库。
  * 要求三：用 spark 算子的方式实现上述的统计，存储到磁盘。
  */
object proCity {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    // 判断路径是否正确
    if(args.length != 2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath,outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    sQLContext.setConf("spark.sql.parquet.compression.codec","snappy")

    val df = sQLContext.read.parquet(inputPath)
    df.registerTempTable("log")
    val result = sQLContext.sql("select provincename,cityname,count(*) from log group by provincename,cityname")

    //要求一：将统计的结果输出成 json 格式，并输出到磁盘目录。
    result.coalesce(1).write.partitionBy("provincename","cityname").json(outputPath)

    //要求二：将结果写到到 mysql 数据库。
    // 加载配置文件  需要使用对应的依赖
    val load = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    result.write.mode("append").jdbc(load.getString("jdbc.url"),load.getString("jdbc.TableName"),prop)

    //要求三：用 spark 算子的方式实现上述的统计，存储到磁盘。
    import sQLContext.implicits._
    val proAndCity2Sum = df.map(row =>{
      val pro = row.getAs[String]("provincename")
      val city = row.getAs[String]("cityname")
      (pro+"-"+city,1)
    }).rdd.reduceByKey(_ + _)

    val res = proAndCity2Sum.map(tup => {
      val arr: Array[String] = tup._1.split("-")
      //(pro,city,sum)
      (arr(0),arr(1),tup._2)
    }).groupBy(_._1)

    sc.stop()
  }

}
