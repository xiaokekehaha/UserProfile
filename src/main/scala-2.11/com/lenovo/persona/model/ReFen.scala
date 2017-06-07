//package com.lenovo.persona.model
//
//
//// scalastyle:off
//import org.apache.spark.sql.Row
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.types.{StringType, StructField, StructType}
//
//object ReFen {
//
//  def main(args: Array[String]): Unit = {
//
//    val sparkConf = new SparkConf().setAppName("Chaifen")
//    val sc = new SparkContext(sparkConf)
//    val hiveContext = new HiveContext(sc)
//    import hiveContext.sql
//
//    sql("use d_lucp_dw")
//
//    val data = sql("select * from hhl_supertag1").map {
//      case Row(superid: String, id_type: String, id: String, tag: String) => Row(superid, id_type, id, tag)
//    }.repartition(10)
//
//
//    //    val data = sql("select * from hhl_supertag1").repartition(10)
//
//    val structType = StructType(Array(
//      StructField("c1", StringType, true),
//      StructField("c2", StringType, true),
//      StructField("c3", StringType, true),
//      StructField("c4", StringType, true),
//      StructField("c5", StringType, true)
//    ))
//
//    val a = hiveContext.createDataFrame(data, structType)
//    a.registerTempTable("wasd")
//    sql("insert into hhl_supertag2 select * from wasd")
//
//
//  }
//}
