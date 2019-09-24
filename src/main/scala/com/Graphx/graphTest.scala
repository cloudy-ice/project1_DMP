package com.Graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 图计算案例
  *
  */
object graphTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    //点
    val vertexRDD = sc.makeRDD(Seq(
      (1L,("小明",23)),
      (2L,("林俊杰",34)),
      (9L,("霉霉",35)),
      (133L,("抖森",0)),
      (6L,("韩红",55)),
      (16L,("鞠婧祎",24)),
      (21L,("刘诗诗",29)),
      (44L,("肖战",25)),
      (138L,("王一博",26)),
      (5L,("杨幂",34)),
      (7L,("那英",56)),
      (158L,("李连杰",64))
    ))

    //边
    val edgeRDD = sc.makeRDD(Seq(
      Edge(1L,133L,0),
      Edge(2L,133L,0),
      Edge(9L,133L,0),
      Edge(6L,133L,0),
      Edge(6L,138L,0),
      Edge(16L,138L,0),
      Edge(44L,138L,0),
      Edge(21L,138L,0),
      Edge(5L,158L,0),
      Edge(7L,158L,0)
    ))

    //图
    val graph = Graph(vertexRDD,edgeRDD)

    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
    //连通图
    //(2,1)
    //(21,1)
    //(133,1)
    //(1,1)
    //(7,5)
    //(9,1)
    //(5,5)
    //`····
//    vertices.foreach(println)
    //(133,(1,(抖森,0)))
    //(158,(5,(李连杰,64)))
    //(1,(1,(小明,23)))
//    vertices.join(vertexRDD).foreach(println)

    vertices.join(vertexRDD).map{
      case (userId,(cnId,(name,age)))=>(cnId,List((name,age)))
    }
      .reduceByKey(_ ++ _).foreach(println)

  }


}
