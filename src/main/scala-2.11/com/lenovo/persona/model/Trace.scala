//package com.lenovo.persona.model
//
//// scalastyle:off
//import java.text.SimpleDateFormat
//
//import com.lenovo.persona.utils.ConfigUtils
//import org.apache.spark.sql.SparkSession
//import org.json4s.JsonDSL._
//import org.json4s.jackson.JsonMethods.{compact, render}
//
//import scala.util.Random
//
//object Trace {
//
//
//  def main(args: Array[String]): Unit = {
//
//    val path = args(0)
//
//    val spark = SparkSession
//      .builder()
//      .appName("asd")
//      .getOrCreate()
//    val sc = spark.sparkContext
//
//    //    sql("use d_lucp_dw")
//    //    val hiveData = sql("select * from hhl_supertag1 where tag <> 'null'").map {
//    //      case Row(superid: String, id_type: String, id: String, tag: String) => (superid, (id_type, id, tag))
//    //    }.groupByKey()
//
//
//    // {"superid":"1","date":"20160502","theme":"设备使用时长","content":"2"}
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
//  }
//
//  def toJson(src:(String,String,String,String)):String = {
//
//    val js = (
//      ("index" -> ("_id" -> src._1))
//    )
//
//    val json = (
//      ("superid" -> src._1) ~
//        ("date" -> src._2) ~
//        ("theme" -> src._3) ~
//        ("content" -> src._4)
//    )
//    compact(render(js)) +"\n"+compact(render(json))
//  }
//
//  def dateTrans(src:String):String = {
//    val spdf = new SimpleDateFormat("yyyyMMdd")
//    val date = spdf.parse(src)
//    val spdf2 = new SimpleDateFormat("yyyy-MM-dd")
//    spdf2.format(date)
//  }
//
//}