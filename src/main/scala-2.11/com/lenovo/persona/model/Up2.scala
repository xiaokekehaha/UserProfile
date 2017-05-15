///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//// scalastyle:off
//
//package com.lenovo.persona.model
//
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.SparkSession
//import org.paukov.combinatorics.Factory
//import com.lenovo.persona.utils.RDDExe._
//import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession
//import org.apache.spark.graphx.{Graph, VertexId}
//
//import scala.collection.JavaConverters._
//
//object Up2 {
//
//  def main(args: Array[String]): Unit = {
//
//    Logger.getLogger("org").setLevel(Level.OFF)
//    // Creates a SparkSession.
//    val spark = SparkSession
//      .builder
//      .appName(s"${this.getClass.getSimpleName}")
//      .master("local")
//      .getOrCreate()
//    val sc = spark.sparkContext
//
//    val data = sc.textFile("/Users/liziyao/Data/up/sample_data.csv")
//      .flatMap(x => {
//        val line = x.split(",")
//        val itr = List("lenovoid:" + line(0), "customid:" + line(1), "lps_did:" + line(8))
//        val initialVector = Factory.createVector(itr.asJava)
//        var index = 2
//        if (itr.length < 2) {
//          index = itr.length
//        }
//        val result = Factory.createSimpleCombinationGenerator(initialVector, index)
//        result.generateAllObjects().asScala.toList.map {
//          item =>
//            item.asScala.toList.sorted
//        }.map(x => (x(0), x(1)))
//      }).filter(x => {
//      if (x._1.last == ':' || x._2.last == ':')
//        false
//      else
//        true
//    })
//
////    println(data.collect.mkString("\n"))
//
//    val dic = data.flatMap( x => List(x._1,x._2)).zipWithUniqueId()
//
//    val rowEdges = data.join(dic).map(_._2).join(dic).map(_._2)
//
////    val rowEdges = tmp.map( x => (x._2._1,x._1))
////      .groupByKey()
////      .map( x => {
////        val itr = x._2.toList
////        if(itr.size == 1){
////          (itr(0).asInstanceOf[VertexId],itr(0).asInstanceOf[VertexId])
////        } else {
////          (itr(0).asInstanceOf[VertexId],itr(1).asInstanceOf[VertexId])
////        }
////      })
////
//    val graph = Graph.fromEdgeTuples(rowEdges, 1)
////
//    val cc = graph.connectedComponents().vertices
//
//    val ccByUsername = dic.map(_.swap).join(cc).map {
//      case (id, (username, cc)) => (username, cc)
//    }
////
//    val rr = ccByUsername.map(x => (x._2, x._1)).groupByKey().map(x => {
//
//      val id = x._1
//      val mes = x._2.toList.distinct.mkString("|")
//      val sortKey = x._2.size
//
//      (sortKey,(id, mes))
//    }).sortByKey(false)
//
//    // scalastyle:off
//    println(rr.map(_._2).collect().mkString("\n"))
//
//
//  }
//
//}
