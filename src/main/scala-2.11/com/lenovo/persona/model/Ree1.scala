package com.lenovo.persona.model

// scalastyle:off
import com.lenovo.persona.utils.ConfigUtils
import org.apache.spark.sql.SparkSession
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}
import scala.collection.JavaConverters._


object Ree1 {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val data = spark.sql("select sid,concat_ws(';',collect_set(concat(idtype,'|',id))) ids,concat_ws(';',collect_set(tag)) tags from d_lucp_dw.temp_tag group by sid")

    val hiveData = data.map(x => (x.getAs[String](0), x.get(1).toString, x.getAs[String](2)))


//    hiveData.joinWith(hiveData,hiveData.col("_1") === hiveData.col("_2"))

    val result = hiveData.map(x => {
      val superid = "s_" + x._1
      val ids = x._2.split(";").toList
      val tags = x._3.split(";").toList
      //val count = tags.size
//      superid+"\005"+toJson_ids(superid, ids, tags)
      (superid,toJson_ids(superid, ids, tags))
      //      superid+","+count
    })
    result.toDF("sid", "tagjosn").createOrReplaceTempView("table1")
//     ConfigUtils.overwriteTextFile("ree1",result.rdd)
    //spark.sql("create table proj_gucp_dw.tmp_tag_final_2 as select * from table1")
    spark.sql("create table d_lucp_dw.tmp_tag_final as select * from table1")
  }

  def toJson_ids(superid: String, list_ids: List[String], list_tags: List[String]): String = {
    val groupList_ids = list_ids.map(_.split("\\|")).filter( x => x.size == 2)
      .map(tup => {
      val id = tup(0)
      val value = tup(1)
      (id, value)
    }).groupBy(_._1).map(x => {
      val key = x._1
      val value = x._2.map(_._2)
      (key, value)
    })
    val groupList_tags = list_tags.map(_.split(":"))
        .filter(x => x.size == 3)
        .map(tup => {
          val tag_id = tup(0)
          val tag_code = tup(1)
          val tag_w = tup(2)
          (tag_id, (tag_code, tag_w))
        })
        .groupBy(_._1).map(x => {
        val tag_id = x._1
        val code_v = x._2.map(_._2)
        (tag_id, code_v)
      })

    val lenovoid = groupList_ids.getOrElse("lenovoid", List(""))
    val service_users_id = groupList_ids.getOrElse("service_users_id", List(""))
    val microblogid = groupList_ids.getOrElse("microblogid", List(""))
    val wechatid = groupList_ids.getOrElse("wechatid", List(""))
    val cp = groupList_ids.getOrElse("cp", List(""))
    val email = groupList_ids.getOrElse("email", List(""))
    val unique_cookie = groupList_ids.getOrElse("unique_cookie", List(""))
    val imei_num = groupList_ids.getOrElse("imei", List(""))
    val lps_did = groupList_ids.getOrElse("lps_did", List(""))
    //    val indexJson =(
    //      ("index" -> ("_id" -> superid))
    //      )
    val indexJson = ""
    val json =
      (("superid" -> List(superid)) ~
        ("lenovoid" -> lenovoid) ~
        ("service_users_id" -> service_users_id) ~
        ("microblogid" -> microblogid) ~
        ("wechatid" -> wechatid) ~
        ("cp" -> cp) ~
        ("email" -> email) ~
        ("unique_cookie" -> unique_cookie) ~
        ("imei_num" -> imei_num) ~
        ("lps_did" -> lps_did) ~
        groupList_tags.map { x =>
          ((x._1 -> x._2.map { y =>
            (("value" -> y._1) ~
              ("weight" -> y._2))
          }))
        }
        )
    //    compact(render(indexJson)) + compact(render(json))
    //compact(render("") +  compact(render(json)))
    compact(render(json))
  }

}

