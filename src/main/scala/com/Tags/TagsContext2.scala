package com.Tags

import com.Utils.TagUtils
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 解决统一用户识别问题
  */
object TagsContext2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "F:\\hadoop-2.7.2")
    if(args.length != 4){
      println("目录参数不匹配，退出程序")
      sys.exit()
    }
    val Array(inputPath,app_dictPath,stopPath,days) = args
    val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getName)
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    //调用HbaseAPI
    val load = ConfigFactory.load()
    //获取表名
    val HbaseTableName = load.getString("HBASE.tableName")
    //创建Hadoop任务
    val configuration = spark.sparkContext.hadoopConfiguration
    //配置Hbase连接
    configuration.set("hbase.zookeeper.quorum",load.getString("HBASE.Host"))
    //获取connection连接
    val hbConn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbConn.getAdmin
    ///判断当前表是否被使用
    if(!hbadmin.tableExists(TableName.valueOf(HbaseTableName))){
      print("当前表可用")
      //创建表对象
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(HbaseTableName))
      //创建列簇
      val hColumnDescriptor = new HColumnDescriptor("tags")
      //将创建好的列簇加入表中
      tableDescriptor.addFamily(hColumnDescriptor)
      hbadmin.createTable(tableDescriptor)
      hbadmin.close()
      hbConn.close()
    }
    val config = new JobConf(configuration)
    //指定输出类型
    config.setOutputFormat(classOf[TableOutputFormat])
    //指定输出哪张表
    config.set(TableOutputFormat.OUTPUT_TABLE,HbaseTableName)

    val df = spark.read.parquet(inputPath)
    //读取字典文件(url,appname)
    val dict = spark.read.textFile(app_dictPath).map(_.split("\t",-1))
      .filter(_.length >= 5).map(arr => (arr(4),arr(1))).rdd.collectAsMap()
    val bc_dict = sc.broadcast(dict)

    //获取停用词
    val stopword = sc.textFile(stopPath).map((_,0)).collectAsMap()
    val bc_stopword = sc.broadcast(stopword)
    //获取所有ID
    val allUserId = df.rdd.map(row =>{
      val strList = TagUtils.getAllUserId(row)
      (strList,row)
    })

    //构建点集合
    // 构建点集合
    val verties = allUserId.flatMap(row=>{
      // 获取所有数据
      val rows = row._2

      val adList = TagsAd.makeTags(rows)
      // 商圈
      val businessList = BusinessTag.makeTags(rows)
      // 媒体标签
      val appList = TagsApp.makeTags(rows,bc_dict)
      // 设备标签
      val devList = TagsDevice.makeTags(rows)
      // 地域标签
      val locList = TagsLocation.makeTags(rows)
      // 关键字标签
      val kwList = TagsKeyWord.makeTags(rows,bc_stopword)
      // 获取所有的标签
      val tagList = adList++ appList++devList++locList++kwList
      // 保留用户Id
      val VD = row._1.map((_,0))++tagList
      // 思考  1. 如何保证其中一个ID携带着用户的标签
      //     2. 用户ID的字符串如何处理
      row._1.map(uId=>{
        if(row._1.head.equals(uId)){
          (uId.hashCode.toLong,VD)
        }else{
          (uId.hashCode.toLong,List.empty)
        }
      })
    })
    // 打印
    //verties.take(20).foreach(println)
    // 构建边的集合
    val edges = allUserId.flatMap(row=>{
      // A B C: A->B  A ->C
      row._1.map(uId=>Edge(row._1.head.hashCode.toLong,uId.hashCode.toLong,0))
    })
    //edges.foreach(println)
    // 构建图
    val graph = Graph(verties,edges)
    // 根据图计算中的连通图算法，通过图中的分支，连通所有的点
    // 然后在根据所有点，找到内部最小的点，为当前的公共点
    val vertices = graph.connectedComponents().vertices
    // 聚合所有的标签
    vertices.join(verties).map{
      case (uid,(cnId,tagsAndUserId))=>{
        (cnId,tagsAndUserId)
      }
    }.reduceByKey(
      (list1,list2)=>{
        (list1++list2)
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum)
          .toList
      }).map{
      case (userId,userTags) =>{
        // 设置rowkey和列、列名
        val put = new Put(Bytes.toBytes(userId))
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(days),Bytes.toBytes(userTags.mkString(",")))
        (new ImmutableBytesWritable(),put)
      }
    }.saveAsHadoopDataset(config)

    spark.stop()
    sc.stop()
  }
}
