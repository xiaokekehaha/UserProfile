//package com.lenovo.persona.model
//
//// scalastyle:off
//import com.lenovo.persona.utils.ConfigUtils
//import org.apache.spark.sql.SparkSession
//import org.json4s.JsonDSL._
//import org.json4s.jackson.JsonMethods.{compact, render}
//
//object ReeLPE {
//
//
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession
//      .builder()
//      .appName("asd")
//      .master("local")
//      .getOrCreate()
//
//    val sc = spark.sparkContext
//
//    val new_tag = sc.textFile("/Users/liziyao/Downloads/new_tag.txt").map(_.split("\001")).map(x => (x(0), (x(1), x(2), x(3))))
//
//    val hiveData_1 = sc.textFile("/Users/liziyao/Downloads/reee.txt").map(_.split("\001")).map(x => (x(0), (x(1), x(2), x(3)))).filter( x => x._2._1 != "lps_did").union(new_tag)
//
//
////    val newData = sc.textFile("/Users/liziyao/Downloads/reee.txt").map(_.split("\001")).map(x => ((x(0),x(1), x(2)), x(3))).map( x => {
////      val first = x._1
////      var value = x._2
////      if (new_tag.contains(first))
////        value = new_tag.get(first).get
////      (first._1,(first._2,first._3,value))
////    })
//
//
//    val lenovo_super = sc.textFile("/Users/liziyao/Downloads/reee.txt").map(_.split("\001")).map(x => (x(0), x(1), x(2))).filter(x => x._2 == "lenovoid").distinct().map(x => (x._3, x._1))
//
//    val tmp = sc.textFile("/Users/liziyao/Downloads/tmp_it_lenovoid_result.txt").map(_.split("\001")).map(x => (x(0), x(1))).join(lenovo_super).map(x => {
//      val lenovoid = x._1
//      val superid = x._2._2
//      val id = x._2._1
//      var id_type = ""
//      if (id.contains("@"))
//        id_type = "email"
//      else
//        id_type = "mobile_md5"
//      (superid, (id_type, id, "noweight"))
//    })
//
//    val hiveData_union = hiveData_1.union(tmp)
//
//    //    ConfigUtils.overwriteTextFile("user/latong",hiveData_union.map( x => ))
//
//    val hiveData = hiveData_union.groupByKey()
//
//    val result = hiveData.map(x => {
//      val superid = x._1
//      val ids = x._2.map(x => x._1 + ":" + x._2).toList.distinct
//      val tags = x._2.map(_._3).toList.filter(x => x != "noweight").distinct
//      val count = tags.map(_.split(":")(0)).distinct.size
//      toJson_ids(superid, ids, tags)
//      //      superid + "," + count
//    })
//
//
//    val result_count = hiveData.map(x => {
//      val superid = x._1
//      val ids = x._2.map(x => x._1 + ":" + x._2).toList.distinct
//      //      val tags = x._2.map(_._3).toList.filter( x => x != "noweight")
//      //      val count = tags.map(_.split(":")(0)).distinct.size
//      toJson_ids_2(superid, ids)
//      //            superid + "," + count
//    })
//
//    //    ConfigUtils.overwriteTextFile("user/reee",result)
//    //    ConfigUtils.overwriteTextFile("user/re_count",result)
//    result.saveAsTextFile("/Users/liziyao/Downloads/re_tag_new")
//    //    result_count.saveAsTextFile("/Users/liziyao/Downloads/re_count")
//
//
//  }
//
//
//  def toJson_ids_2(superid: String, list_ids: List[String]): String = {
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
//    val indexJson = (
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
//        ("lps_did" -> lps_did)
//        )
//    compact(render(indexJson)) + "\n" + compact(render(json))
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
//    val indexJson = (
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
//        groupList_tags.map { x =>
//          ((x._1 -> x._2.map { y =>
//            (("value" -> y._1) ~
//              ("weight" -> y._2))
//          }))
//        }
//        )
//    compact(render(indexJson)) + "\n" + compact(render(json))
//  }
//
//}
