//package com.lenovo.persona.model
//
//
//// scalastyle:off
//import com.lenovo.persona.utils.ConfigUtils
//import org.apache.spark.graphx.Graph
//import org.apache.spark.sql.{Row, SparkSession}
//import org.json4s.jackson.JsonMethods.{compact, render}
//import org.json4s.JsonDSL._
//
//object UpDs {
//
//  def main(args: Array[String]): Unit = {
//
//    // SparkSession init
//    val spark = SparkSession
//      .builder()
//      .appName("asd")
//      .enableHiveSupport()
//      .getOrCreate()
//
//
//    //
//    val data = spark.sql("select distinct * from d_lucp_dw.hhl_mid")
//      .rdd
//      .map(x => (x.getAs[String](0).trim, x.getAs[String](1).trim))
//
//    val dic = data.flatMap(x => List(x._1, x._2)).distinct().zipWithUniqueId().cache()
//
//
//    val rowEdges = data.join(dic).map(_._2).join(dic).map(_._2).map(x => {
//      if (x._1 > x._2)
//        x.swap
//      else
//        x
//    })
//
//
//    val graph = Graph.fromEdgeTuples(rowEdges, 1)
//    val cc = graph.connectedComponents(100).vertices
//
////    val a  = graph.connectedComponents(100)
//
//    val ccByUsername = dic.map(_.swap).join(cc).map {
//      case (id, (username, cc)) => (username, cc)
//    }
//
//
//    val rr = ccByUsername.map(x => (x._2, x._1)).groupByKey().map(x => {
//      val id = x._1
//      val mes = x._2.toList.distinct
//      id + "\005" + toJson(mes)
//      //      (id,toJson(mes))
//    })
//
//    ConfigUtils.overwriteTextFile("user/result",rr)
//
//
//  }
//
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
//    val lenovoid = groupList.getOrElse("1", List(""))
//    val cs_customerid = groupList.getOrElse("2", List(""))
//    val microblogid = groupList.getOrElse("3", List(""))
//    val wechatid = groupList.getOrElse("4", List(""))
//    val mobile_md5 = groupList.getOrElse("5", List(""))
//    val email = groupList.getOrElse("6", List(""))
//    val unique_cookie = groupList.getOrElse("7", List(""))
//    val imei_num = groupList.getOrElse("8", List(""))
//    val lps_did = groupList.getOrElse("9", List(""))
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
//
//}
