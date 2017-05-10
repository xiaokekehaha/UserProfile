package com.lenovo.persona.model

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.json4s._
import org.json4s.native.JsonMethods._

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

    spark.sql("use databaseName")

    val hiveData = spark.sql("select superid,json from table1").map {
      case Row(superid:String,json:String) => (superid,json)
    }.rdd


    // 10009063788 lenovoid {tag_msg}
    val tagData = spark.sql("select id,id_type,json from table2").map{
      case Row(id:String,id_type:String,json:String) => (id,id_type,json)
    }.rdd


    hiveData.map( x => {
      val superid = x._1
      val json = parse(x._2)
    })



  }

}
