package com.Tags

import com.Utils.TagUtils
import com.typesafe.config.{ConfigBeanFactory, ConfigFactory}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
//上下文标签
//存储到hbase
object TagsContext {
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
    //过滤符合ID的数据
    df.filter(TagUtils.OneUserId)
      //接下来所有的标签都在内部实现
      .map(row =>{
      //取出用户id
      val userId = TagUtils.getOneUserId(row)
      //接下来通过rows找到并打上所有标签
      val adList = TagsAd.makeTags(row)
      val appList = TagsApp.makeTags(row,bc_dict)
      val keywordList = TagsKeyWord.makeTags(row,bc_stopword)
      val dvList = TagsDevice.makeTags(row)
      val locationList = TagsLocation.makeTags(row)
      val business = BusinessTag.makeTags(row)

      (userId,adList++appList++keywordList++dvList++locationList++business)
    })
      .rdd.reduceByKey((list1,list2) => {
      // List(("lN插屏",1),("LN全屏",1),("ZC沈阳",1),("ZP河北",1)....)
      (list1 ++ list2)
      //List(("LN插屏")，List（1,1,1,....))
        .groupBy(_._1)
        .mapValues(_.foldLeft[Int](0)(_+_._2))
        .toList
    }).map{
      case(userid,userTag)=>{

        val put = new Put(Bytes.toBytes(userid))
        // 处理下标签
        val tags = userTag.map(t=>t._1+","+t._2).mkString(",")
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(s"$days"),Bytes.toBytes(tags))
        (new ImmutableBytesWritable(),put)
      }
    }.saveAsHadoopDataset(config)
  }
}
