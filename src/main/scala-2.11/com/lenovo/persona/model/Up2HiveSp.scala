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
//// Todo: 需要将结果json化
//
//package com.lenovo.persona.model
//
//import java.util.logging.{Level, Logger}
//
//import com.lenovo.persona.utils.ConfigUtils
//import org.apache.spark.graphx.Graph
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.types.{StringType, StructField, StructType}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.json4s.JsonDSL._
//import org.json4s.jackson.JsonMethods._
//import org.paukov.combinatorics.Factory
//import com.lenovo.persona.utils.RDDExe._
//import org.apache.spark.rdd.RDD
//
//import scala.collection.JavaConverters._
//import scala.collection.mutable.ArrayBuffer
//import scala.util.Random
//
//object Up2HiveSp {
//
//  case class Relation(super_id: String, relation: String)
//
//  def main(args: Array[String]): Unit = {
//
////    Logger.getLogger("org").setLevel(Level.OFF)
//
//    val sparkConf = new SparkConf()
//      .setAppName("Fst_Step_Graph_Components")
//      .set("spark.broadcast.compress", "true")
//      .set("spark.io.compression.codec","lzf")
//    val sc = new SparkContext(sparkConf)
//
//
//    //    val hiveContext = new HiveContext(sc)
//    //    //    val sqlContext = new SQLContext(sc)
//    //    import hiveContext.sql
//    //
//    //    sql("use d_lucp_dw")
//    //    sql("insert into log select 'program step1--init spark' from dual")
//    //
//    //    val hiveData = sql("select lenovoid,cs_customerid,microblogid,wechatid,mobile_md5,email,unique_cookie,imei_num,lps_did from user_rel").rdd.map(x => {
//    //      val lenovoid = x.getAs[String]("lenovoid")
//    //      val cs_customerid = x.getAs[String]("cs_customerid")
//    //      val microblogid = x.getAs[String]("microblogid")
//    //      val wechatid = x.getAs[String]("wechatid")
//    //      val mobile_md5 = x.getAs[String]("mobile_md5")
//    //      val email = x.getAs[String]("email")
//    //      val unique_cookie = x.getAs[String]("unique_cookie")
//    //      val imei_num = x.getAs[String]("imei_num")
//    //      val lps_did = x.getAs[String]("lps_did")
//    //      (lenovoid, cs_customerid, microblogid, wechatid, mobile_md5, email, unique_cookie, imei_num, lps_did)
//    //    })
//    //
//    ////    val hiveCount = hiveData.count()
//    ////    sql(s"insert into log select 'hive-count:$hiveCount' from dual")
//    //
//    //    val data = hiveData
//    //      .flatMap(x => {
//    //        val itr = List("lenovoid:" + x._1, "cs_customerid:" + x._2, "microblogid:" + x._3,
//    //          "wechatid:" + x._4, "mobile_md5:" + x._5, "email:" + x._6, "unique_cookie:" + x._7, "imei_num:" + x._8, "lps_did:" + x._9)
//    //        val initialVector = Factory.createVector(itr.asJava)
//    //        var index = 2
//    //        if (itr.length < 2) {
//    //          index = itr.length
//    //        }
//    //        val result = Factory.createSimpleCombinationGenerator(initialVector, index)
//    //        result.generateAllObjects().asScala.toList.map {
//    //          item =>
//    //            item.asScala.toList.sorted
//    //        }.map(x => (x(0), x(1)))
//    //      }).filter(x => {
//    //      if (x._1.last == ':' || x._2.last == ':')
//    //        false
//    //      else
//    //        true
//    //    }).cache()
//    //
//    ////    val dataCount = data.count()
//    ////    sql(s"insert into log select 'data-count:$dataCount' from dual")
//    //
//    //    val dic = data.flatMap(x => List(x._1, x._2)).distinct().zipWithUniqueId().cache()
//    //    sql(s"insert into log select 'dic-created' from dual")
//    //
//
//    //    val data = sql("select * from user_relation").rdd
//
//    val data = sc.textFile("rowww")
//    //
//    //    val e = args(0).toInt
//
//    //    val list = new ArrayBuffer[RDD[String]]()
//    //
//    //    for (i <- 0 to 10) {
//    //      list.append(data)
//    //    }
//    //
//    //    val seq = list.toSeq
//
//
//    val dd = data.union(data)
//
//    val rowEdges = dd.map(x => {
//      val a = Random.nextLong()
//      val b = Random.nextLong()
//      a + "," + b
//    })
//
//    ConfigUtils.overwriteTextFile("rowwww", rowEdges)
//
//    // cun yixia bian
//    //    val structType2 = StructType(Array(
//    //      StructField("edge1", StringType, true),
//    //      StructField("edge2", StringType, true)
//    //    ))
//
//    //    val b = hiveContext.createDataFrame(rowEdges.map(x => Row(x._1.toString, x._2.toString)),structType2)
//    //    b.show()
//    //    b.registerTempTable("row")
//    //    sql("insert into zy_row_edge select * from row")
//
//    //    sql(s"insert into log select 'dropTempTable' from dual")
//    //    hiveContext.dropTempTable("row")
//
//    //    sql(s"insert into log select 'start-graph' from dual")
//
//
//    val n = args(0).toInt
//    val asd = sc.textFile("rowwww").map(_.split(",")).map(x => (x(0).toLong, x(1).toLong))
//
//    var s1 = System.currentTimeMillis()
//    val graph = Graph.fromEdgeTuples(asd, 1)
//    println("造图************:" + (System.currentTimeMillis() - s1))
//
//    s1 = System.currentTimeMillis()
//    val cc = graph.connectedComponents()
//    val aa = cc.vertices
//    println("子图************:" + (System.currentTimeMillis() - s1))
//
//    sc.stop()
//
//
//    //    sql(s"insert into log select 'step122' from dual")
//    //    val ccByUsername = dic.map(_.swap).join(cc).map {
//    //      case (id, (username, cc)) => (username, cc)
//    //    }
//    //
//    //    sql(s"insert into log select 'step123' from dual")
//    //    val rr = ccByUsername.map(x => (x._2, x._1)).groupByKey().map(x => {
//    //      val id = x._1
//    //      val mes = x._2.toList.distinct
//    ////      id + "\005" + toJson(mes)
//    //      (id,toJson(mes))
//    //    }).map(x => Row(x._1.toString, x._2.toString))
//    //
//    //    sql(s"insert into log select 'step124' from dual")
//    //        val structType = StructType(Array(
//    //          StructField("super_id", StringType, true),
//    //          StructField("relation", StringType, true)
//    //        ))
//    //
//    ////    println("the number of result:" + rr.count())
//    ////    val rrCount = rr.count()
//    //    sql(s"insert into log select 'step125' from dual")
//    ////    ConfigUtils.overwriteTextFile("/user/u_lucp_dw/user_graph", rr)
//    //
//    //        val a = hiveContext.createDataFrame(rr,structType)
//    //
//    //        a.registerTempTable("wasd")
//    //    sql(s"insert into log select 'step126' from dual")
//    ////        rr.toDF().createOrReplaceTempView("relation")
//    //    sql("insert into super_v1 select * from wasd")
//    //    sql(s"insert into log select 'step127' from dual")
//
//
//    // Todo: Save???
//
//
//  }
//
//  def toJson(list: List[String]): String = {
//
//    val groupList = list.map(x => {
//      val tup = x.split(":")
//      val id = tup(0)
//      val value = tup(1)
//      (id, value)
//    }).groupBy(_._1).map(x => {
//      val key = x._1
//      val value = x._2.map(_._2)
//      (key, value)
//    })
//
//    val lenovoid = groupList.getOrElse("lenovoid", List(""))
//    val cs_customerid = groupList.getOrElse("cs_customerid", List(""))
//    val microblogid = groupList.getOrElse("microblogid", List(""))
//    val wechatid = groupList.getOrElse("wechatid", List(""))
//    val mobile_md5 = groupList.getOrElse("mobile_md5", List(""))
//    val email = groupList.getOrElse("email", List(""))
//    val unique_cookie = groupList.getOrElse("unique_cookie", List(""))
//    val imei_num = groupList.getOrElse("imei_num", List(""))
//    val lps_did = groupList.getOrElse("lps_did", List(""))
//
//    val json =
//      (
//        ("lenovoid" -> lenovoid) ~
//          ("cs_customerid" -> cs_customerid) ~
//          ("microblogid" -> microblogid) ~
//          ("wechatid" -> wechatid) ~
//          ("mobile_md5" -> mobile_md5) ~
//          ("email" -> email) ~
//          ("unique_cookie" -> unique_cookie) ~
//          ("imei_num" -> imei_num) ~
//          ("lps_did" -> lps_did)
//        )
//    compact(render(json))
//  }
//
//}
