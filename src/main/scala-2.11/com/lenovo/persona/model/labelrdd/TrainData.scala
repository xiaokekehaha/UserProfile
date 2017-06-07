//package com.lenovo.persona.model.labelrdd
//
//// scalastyle:off
//
//import org.apache.spark.sql.SparkSession
//
//object TrainData {
//
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession.builder().appName("asd").master("local").getOrCreate()
//    import spark.implicits._
//
//    val data = spark.read.textFile("/Users/liziyao/Data/UserProfile/pc_ace.txt")
//      .map(_.split("\001"))
//      .map(x => {
//        val lps_did = x(0)
//        val os_version = x(2)
//        val lang = x(3)
//        val reso = x(4)
//        val city = x(8) + "市"
//        val soft_type = x(12)
//        (lps_did, (os_version, lang, reso, city, soft_type))
//      }).rdd
//
//    val groupd = data.groupByKey().map(x => {
//      val list = x._2.toList
//      val ad = list(0)
//      val os_code = ad._1
//      val lang_code = ad._2
//      val reso_code = ad._3
//      val city_code = ad._4
//      val softs = list.map(_._5).distinct.mkString("-")
//      os_code + "," + lang_code + "," + reso_code + "," + city_code + "," + softs + ","
//    })
//
//    groupd.saveAsTextFile("/Users/liziyao/Desktop/Train")
//  }
//
//}
