///*
// * Lenovo Persona Project
// */
//
//
// scalastyle:off
//package com.lenovo.persona.model
//
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.graphx.{Graph, VertexId}
//import org.apache.spark.sql.SparkSession
//import org.paukov.combinatorics.Factory
//
//import scala.collection.JavaConverters._
//
//object Up {
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.OFF)
//    // Creates a SparkSession.
//    val spark = SparkSession
//      .builder
//      .appName(s"${this.getClass.getSimpleName}")
//      .master("local")
//      .getOrCreate()
//    val sc = spark.sparkContext
//    //    sc.setLogLevel("OFF")
//
//    val data = sc.textFile("/Users/liziyao/Data/up/sample_data.csv")
//      .map(x => {
//        val line = x.split(",")
//        ("lenovoid:" + line(0), "customid:" + line(1), "lps_did:" + line(8))
//      }).zipWithUniqueId()
//
//    val dic = data.map(x => (x._2, x._1))
//
//    val data_one = data.filter(x => x._1._3 == "lps_did:")
//    val data_more = data.filter(x => x._1._3 != "lps_did:")
//
//    // construct subs
//    val data_more_double = data_more.map(x => (x._1._3, x)).groupByKey().flatMap(x => {
//      val emi = x._1
//      val itr = x._2.map(_._2).toList
//      val initialVector = Factory.createVector(itr.asJava)
//      var index = 2
//      if (itr.length < 2) {
//        index = itr.length
//      }
//      val result = Factory.createSimpleCombinationGenerator(initialVector, index)
//      result.generateAllObjects().asScala.toList.map {
//        item =>
//          item.asScala.toList.sorted
//      }.map(x => (x(0), x(1)))
//    })
//
//    // scalastyle:off
//    val data_one_double = data_one.map(x => (x._2, x._2))
//
//    val data_union = data_more_double union data_one_double
//
//    val rowEdges = data_union.map(x => {
//      (x._1.asInstanceOf[VertexId], x._2.asInstanceOf[VertexId])
//    })
//
//    val graphUp = Graph.fromEdgeTuples(rowEdges, 1)
//
//    val cc = graphUp.connectedComponents().vertices
//
//    val ccByUsername = dic.join(cc).map {
//      case (id, (username, cc)) => (username, cc)
//    }
//
//
//    val rr = ccByUsername.map(x => (x._2, x._1)).groupByKey().map(x => {
//
//      val id = x._1
//      val mes = x._2.flatMap(x=>List(x._1,x._2,x._3)).toList.filter( x => {
//        if (x.last == ':')
//          false
//        else
//          true
//      }).distinct.mkString("|")
//      val sortKey = x._2.size
//
//      (sortKey,(id, mes))
//    }).sortByKey(false)
//
//
//    println(rr.map(_._2).collect().mkString("\n"))
//  }
//}
//
//// scalastyle:on println
