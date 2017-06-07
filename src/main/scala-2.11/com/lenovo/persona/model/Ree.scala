//package com.lenovo.persona.model
//
//// scalastyle:off
//import com.lenovo.persona.utils.ConfigUtils
//import org.apache.spark.sql.{Row, SparkSession}
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.types.{StringType, StructField, StructType}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.json4s.JsonDSL._
//import org.json4s.jackson.JsonMethods.{compact, render}
//
//object Ree {
//
//
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession
//      .builder()
//      .appName("asd")
//      .getOrCreate()
//
//    val sc = spark.sparkContext
//
////    sql("use d_lucp_dw")
//
////    val hiveData = sql("select * from hhl_supertag1 where tag <> 'null'").map {
////      case Row(superid: String, id_type: String, id: String, tag: String) => (superid, (id_type, id, tag))
////    }.groupByKey()
//
//    val hiveData = sc.textFile("reee.txt").map(_.split("\001")).map( x =>(x(0),(x(1),x(2),x(3)))).groupByKey()
//
//    val result = hiveData.map(x => {
//      val superid = x._1
//      val ids = x._2.map(x => x._1 + ":" + x._2).toList.distinct
//      val tags = x._2.map(_._3).toList
//      val count = tags.size
//      toJson_ids(superid,ids,tags)
////      superid+","+count
//    })
//
//    ConfigUtils.overwriteTextFile("user/reee",result)
//
//
//  }
//
//
//  def toJson_ids(superid: String, list_ids: List[String], list_tags: List[String]): String = {
//
//    val groupList_ids = list_ids.map(x => {
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
//    val groupList_tags = list_tags.map(x => {
//      val tup = x.split(":")
//      val tag_id = tup(0)
//      val tag_code = tup(1)
//      val tag_w = tup(2)
//      (tag_id, (tag_code, tag_w))
//    }).groupBy(_._1).map(x => {
//      val tag_id = x._1
//      val code_value = x._2.map(_._2)
//      (tag_id, code_value)
//    })
//
//    val lenovoid = groupList_ids.getOrElse("lenovoid", List(""))
//    val cs_customerid = groupList_ids.getOrElse("cs_customerid", List(""))
//    val microblogid = groupList_ids.getOrElse("microblogid", List(""))
//    val wechatid = groupList_ids.getOrElse("wechatid", List(""))
//    val mobile_md5 = groupList_ids.getOrElse("mobile_md5", List(""))
//    val email = groupList_ids.getOrElse("email", List(""))
//    val unique_cookie = groupList_ids.getOrElse("unique_cookie", List(""))
//    val imei_num = groupList_ids.getOrElse("imei_num", List(""))
//    val lps_did = groupList_ids.getOrElse("lps_did", List(""))
//
//
//    val indexJson =(
//      ("index" -> ("_id" -> superid))
//      )
//
//    val json =
//      (("superid" -> superid) ~
//        ("lenovoid" -> lenovoid) ~
//        ("cs_customerid" -> cs_customerid) ~
//        ("microblogid" -> microblogid) ~
//        ("wechatid" -> wechatid) ~
//        ("mobile_md5" -> mobile_md5) ~
//        ("email" -> email) ~
//        ("unique_cookie" -> unique_cookie) ~
//        ("imei_num" -> imei_num) ~
//        ("lps_did" -> lps_did) ~
//        groupList_tags.map{ x =>
//          ((x._1 -> x._2.map{ y =>
//            (("value" -> y._1) ~
//              ("weight" -> y._2))
//          }))
//        }
//        )
//    compact(render(indexJson)) + compact(render(json))
//  }
//
//}
