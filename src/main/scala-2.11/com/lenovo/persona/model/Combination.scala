package com.lenovo.persona.model

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._

object Combination {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setAppName("item_item")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)
    import hiveContext.sql

    sql("use d_lucp_dw")

    val hiveData = sql("select super_id,relation from superid_v1").map {
      case Row(superid: String, json: String) => (superid, json)
    }

    // 10009063788 lenovoid {tag_msg}
    val tagData = sql("select id,id_type,json from table2").map {
      case Row(id: String, id_type: String, json: String) => (id, id_type, json)
    }

    val id_superid = hiveData.flatMap(x => {
      val superid = x._1
      val json = x._2
      val lenovoids = parse(json) \ "lenovoid" \\ classOf[JString]
      val cs_customerid = parse(json) \ "cs_customerid" \\ classOf[JString]
      val microblogid = parse(json) \ "microblogid" \\ classOf[JString]
      val wechatid = parse(json) \ "wechatid" \\ classOf[JString]
      val mobile_md5 = parse(json) \ "mobile_md5" \\ classOf[JString]
      val email = parse(json) \ "email" \\ classOf[JString]
      val unique_cookie = parse(json) \ "unique_cookie" \\ classOf[JString]
      val imei_num = parse(json) \ "imei_num" \\ classOf[JString]
      val lps_did = parse(json) \ "lps_did" \\ classOf[JString]
      List(
        ("lenovoid:", lenovoids, superid),
        ("cs_customerid:", cs_customerid, superid),
        ("microblogid:", microblogid, superid),
        ("wechatid:", wechatid, superid),
        ("mobile_md5:", mobile_md5, superid),
        ("email:", email, superid),
        ("unique_cookie:", unique_cookie, superid),
        ("imei_num:", imei_num, superid),
        ("lps_did:", lps_did, superid)
      )
    }).filter(x => {
      if (x._2(0) == "") false else true
    }).flatMap(x => {
      val id_type = x._1
      val ids = x._2
      val super_id = x._3
      ids.map(x => (id_type + x, super_id))
    })

    val id_json = tagData.map(x => (x._2 + ":" + x._1, x._3))

    val result = id_superid.leftOuterJoin(id_json).map(x => {
      val id = x._1
      val super_id = x._2._1

      // json: 123456:0.7
      val json = x._2._2.getOrElse("")
      (super_id, json)
    }).groupByKey().
  }
}