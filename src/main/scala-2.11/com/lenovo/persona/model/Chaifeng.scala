//package com.lenovo.persona.model
//
//// scalastyle:off
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.types.{StringType, StructField, StructType}
//
//object Chaifeng {
//
//  def main(args: Array[String]): Unit = {
//
//
//    val sparkConf = new SparkConf().setAppName("Chaifen")
//    val sc = new SparkContext(sparkConf)
//    val hiveContext = new HiveContext(sc)
//    import hiveContext.sql
//
//    sql("use d_lucp_dw")
//
//    val hiveData = sql("select * from mid_dmp_tag where tag = 'usetypetagcode'").map {
//      case Row(id: String, tag_id: String, code: String, tag: String,date:String) => (id,tag_id,code,tag,date)
//    }
//
//    val dic_1 = sql("SELECT tagcode,tagcodevalue from mb_tag  where l_table='appuse'").map{
//      case Row(a:String,b:String) => (a,b)
//    }.collectAsMap()
//
//    val dic = sc.broadcast(dic_1)
//
//    val result = hiveData.map( x => {
//      val dicc = dic.value
//      val list = x._3.split(" ")
//
//
//      val zhuan = dicc.getOrElse(x._3,"error")
//      (x._1,x._2,zhuan,x._4,x._5)
//    }).filter( x => x._3 != "error").map( x => Row(x._1,x._2,x._3,x._4,x._5))
//
//    val structType = StructType(Array(
//      StructField("c1", StringType, true),
//      StructField("c2", StringType, true),
//      StructField("c3", StringType, true),
//      StructField("c4", StringType, true),
//      StructField("c5", StringType, true)
//    ))
//
//
//    val a = hiveContext.createDataFrame(result,structType)
//
//    a.registerTempTable("wasd")
//    sql("create table hhl_chaifen as select * from wasd")
//
//
//  }
//
//}
