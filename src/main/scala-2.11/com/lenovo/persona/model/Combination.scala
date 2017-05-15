package com.lenovo.persona.model

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._

object Combination {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .config("spark.sql.warehouse.dir", "/user/hive/071/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    spark.sql("use d_lucp_dw")

    val hiveData = spark.sql("select super_id,relation from superid_v1").map {
      case Row(superid: String, json: String) => (superid, json)
    }.rdd


    // 10009063788 lenovoid {tag_msg}
    val tagData = spark.sql("select id,id_type,json from table2").map {
      case Row(id: String, id_type: String, json: String) => (id, id_type, json)
    }.rdd


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
      List(("lenovoid:", lenovoids, superid), ("cs_customerid:", cs_customerid, superid),
        ("microblogid:", microblogid, superid), ("wechatid:", wechatid, superid),
        ("mobile_md5:", mobile_md5, superid), ("email:", email, superid),
        ("unique_cookie:", unique_cookie, superid),
        ("imei_num:", imei_num, superid))
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
      val json = x._2._2.getOrElse("")
      (super_id, json)
    }).groupByKey()

  }

}
