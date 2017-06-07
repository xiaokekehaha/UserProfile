//package com.lenovo.persona.model
//
//
//// scalastyle:off
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.types.{StringType, StructField, StructType}
//
//object Gp {
//
//  def main(args: Array[String]): Unit = {
//
//    val sparkConf = new SparkConf().setAppName("Gp")
//    val sc = new SparkContext(sparkConf)
//    val hiveContext = new HiveContext(sc)
//    import hiveContext.sql
//
//    sql("use d_lucp_dw")
//
//    val hiveData = sql("select * from hhl_chaifen").map {
//      case Row(c1: String, c2: String, c3: String, c4: String,c5:String) => ((c1,c2),(c3,c4,c5))
//    }.groupByKey().flatMap( x => {
//      val c1 = x._1._1
//      val c2 = x._1._2
//      val c3 = x._2.map( x => x._1).mkString(" ")
//      val line = x._2.map( x => (c1,c2,c3,x._2,x._3))
//      line
//    }).map( x => Row(x._1,x._2,x._3,x._4,x._5))
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
//    val a = hiveContext.createDataFrame(hiveData,structType)
//
//    a.registerTempTable("wasd")
//    sql("create table hhl_chaifen2 as select * from wasd")
//
//
//  }
//
//}
