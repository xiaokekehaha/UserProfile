package com.lenovo.persona.model

// scalastyle:off
import java.text.SimpleDateFormat

import io.searchbox.core.{Bulk, Index}
import org.apache.spark.sql.SparkSession
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}
import toES.ESRestUtil


object Trace {


  def main(args: Array[String]): Unit = {

    val tableName = args(0)

    val ES_LOC_INDEX = "user_trace"
    val ES_LOC_TYPE = tableName match {
      case "mix_all_buy" => "buy_action"
      case "mix_all_posting" => "posting"
      case "mix_browing_forum_count" => "browing_forum_count"
      case "mix_create_lenovo_id" => "create_lenovo_id"
      case "mix_device_input" => "device_input"
      case "mix_lenovo_service_touch" => "service"
      case "mix_log_web_count" => "log_web_count"
      case "mix_log_web_record" => "log_web"
    }

    val spark = SparkSession
      .builder()
      .appName("User_Trace")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    //    val hiveData = sc.textFile(s"trace/$path").map(_.split("\001")).filter( x => x.size == 6).map(x => {
    //      val superid = x(0)
    //      val id_type = x(1)
    //      val id = x(2)
    //      val date = dateTrans(x(3).replaceAll("-",""))
    //      val theme = x(4)
    //      val content = x(5)
    ////      val theme = "设备使用时长"
    ////      val content = Random.nextInt(12) + ""
    //      toJson(superid,date,theme,content)
    //    }
    //    )
    //    ConfigUtils.overwriteTextFile(s"trace_result/$path", hiveData)


    val hiveData = spark.sql(s"select * from d_lucp_dw.$tableName where sid is not null").map(x => {
      val sid = x.getAs[String]("sid")
      val id = x.getAs[String]("id")
      val idtype = x.getAs[String]("idtype")
      val theme = x.getAs[String]("theme")
      val content = x.getAs[String]("content")
      val date = dateTrans(x.getAs[String]("date").replaceAll("-", ""))
      val event = tableName match {
        case "mix_all_buy" => "购买"
        case "mix_all_posting" => "发帖"
        case "mix_browing_forum_count" => ""
        case "mix_create_lenovo_id" => "注册lenovoId"
        case "mix_device_input" => ""
        case "mix_lenovo_service_touch" => "使用联想服务"
        case "mix_log_web_count" => ""
        case "mix_log_web_record" => "登陆"
        case "all_b" => "test"
      }
      val a = (sid, date, theme, content, event)
      toJson(a)
    }).distinct().repartition(20000).cache()

    hiveData.foreachPartition(x => {
      val client = ESRestUtil.getJestClient
      val bulk = new Bulk.Builder().defaultIndex(ES_LOC_INDEX).defaultType(ES_LOC_TYPE)

      x.foreach(msg => {
        val index = new Index.Builder(msg).build()
        bulk.addAction(index)
      }
      )
      client.execute(bulk.build())

    })


  }

  def toJson(src: (String, String, String, String, String)): String = {

    //    val js = (
    //      ("index" -> ("_id" -> src._1))
    //    )
    val sid = "s_"+src._1

    val json =
      ("superid" -> sid) ~
        ("date" -> src._2) ~
        ("theme" -> src._3) ~
        ("content" -> src._4) ~
        ("event" -> src._5)
    compact(render(json))
  }

  def dateTrans(src: String): String = {
    val spdf = new SimpleDateFormat("yyyyMMdd")
    val date = spdf.parse(src)
    val spdf2 = new SimpleDateFormat("yyyy-MM-dd")
    spdf2.format(date)
  }

}