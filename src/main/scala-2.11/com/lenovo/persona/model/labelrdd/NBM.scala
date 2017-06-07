//package com.lenovo.persona.model.labelrdd
//
//// scalastyle:off
//import com.lenovo.persona.utils.ConfigUtils
//import org.apache.spark.mllib.classification.NaiveBayesModel
//import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.mllib.regression.LabeledPoint
//import org.apache.spark.sql.{Row, SparkSession}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.types.{StringType, StructField, StructType}
//
//object NBM {
//
//  def main(args: Array[String]): Unit = {
//
////    val sparkConf = new SparkConf().setAppName("NBM")
////    val sc = new SparkContext(sparkConf)
////    val hiveContext = new HiveContext(sc)
////    //    val sqlContext = new SQLContext(sc)
////    import hiveContext.sql
//
//
//    val spark = SparkSession.builder().appName("asds").master("local").getOrCreate()
//
//    val sc = spark.sparkContext
//
//
//    import spark.implicits._
////    sql("use d_dmp")
//    val data = spark.read.textFile("/Users/liziyao/Downloads/model_data.txt").map( x => {
//      val line = x.split("\001")
////      val lps_did = x.getAs[String]("lps_did")
////      val os_version = x.getAs[String]("os_version")
////      val language = x.getAs[String]("language")
////      val resolution = x.getAs[String]("resolution")
////      val city = x.getAs[String]("city")
////      val software_category = x.getAs[String]("software_category")
//      val lps_did = line(0)
//      val os_version = line(1)
//      val language = line(2)
//      val resolution = line(3)
//      val city = line(4)
//      val software_category = line(5)
//      (lps_did,(os_version,language,resolution,city,software_category))
//    }).rdd.groupByKey().map( x => {
//      val lps_did = x._1
//      val list = x._2.toList
//      val ad = list(0)
//      val os_str = ad._1
//      val os = osVersion(ad._1)
//      val lang = langT(ad._2)
//      val reso = resolution(ad._3)
//      val city = cityCode(ad._4)
//      val softs = list.map(_._5).distinct.map(typeCode)
//      var t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15, t16, t17, t18, t19, t20, t21, t22, t23 = 0
//      softs.map(x => x match {
//        case 1 => t1 = 1
//        case 2 => t2 = 1
//        case 3 => t3 = 1
//        case 4 => t4 = 1
//        case 5 => t5 = 1
//        case 6 => t6 = 1
//        case 7 => t7 = 1
//        case 8 => t8 = 1
//        case 9 => t9 = 1
//        case 10 => t10 = 1
//        case 11 => t11 = 1
//        case 12 => t12 = 1
//        case 13 => t13 = 1
//        case 14 => t14 = 1
//        case 15 => t15 = 1
//        case 16 => t16 = 1
//        case 17 => t17 = 1
//        case 18 => t18 = 1
//        case 19 => t19 = 1
//        case 20 => t20 = 1
//        case 21 => t21 = 1
//        case 22 => t22 = 1
//        case 23 => t23 = 1
//      })
//      (lps_did,os_str,List(os, lang, reso, city, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15, t16, t17, t18, t19, t20, t21, t22,t23))
//    })
//
//    val parsed = data.map(x => {
//      val features = x._3.toArray.map(_.asInstanceOf[Double])
//      val lps_did = x._1
//      val os_version = x._2
//      val vector = Vectors.dense(features)
////      new LabeledPoint(label, vector)
//      (lps_did,os_version,vector)
//    })
//
//
//    val eduModel = NaiveBayesModel.load(sc, "/Users/liziyao/Desktop/NBModel")
//    val salModel = NaiveBayesModel.load(sc, "/Users/liziyao/Desktop/NBModel_2")
//
////    val predictionAndLabel = parsed.map(p => (sameModel.predict(p.features), p.label))
//    val predictionAndLabel = parsed.map(p => (p._1,p._2,eduL(eduModel.predict(p._3).toInt),salaryL(salModel.predict(p._3).toInt)))
//      .map( x => x.productIterator.mkString("\001"))
//
////      .map( x => Row(x._1,x._2,x._3))
//
//    predictionAndLabel.saveAsTextFile("/Users/liziyao/Desktop/ModelRe")
//
////    val structType = StructType(Array(
////      StructField("lps_did", StringType, true),
////      StructField("resolution", StringType, true),
////      StructField("edu", StringType, true),
////      StructField("salary", StringType, true)
////    ))
////
////    val a = hiveContext.createDataFrame(predictionAndLabel,structType)
////    a.registerTempTable("wasd")
////    sql("insert into d_dmp.service_device_model select * from wasd")
//
//  }
//
//  def cityCode(city: String): Int = {
//    val cityMap = ConfigUtils.getConfig("/city.properties")
//    val code = cityMap.getOrElse(city, 7)
//    code.toString.toInt
//  }
//
//  def typeCode(typed: String): Int = {
//    val typeMap = ConfigUtils.getConfig("/pc_soft_type.properties")
//    val code = typeMap.getOrElse(typed, 23)
//    code.toString.toInt
//  }
//
//  def osVersion(osV: String): Int = {
//    osV match {
//      case "Win 10.0" => 0
//      case "Win 6.3" => 1
//      case _ => 2
//    }
//  }
//
//  def langT(lang: String): Int = {
//    lang match {
//      case "zh" => 0
//      case "en" => 1
//      case "ko" => 2
//      case _ => 3
//    }
//  }
//
//  def resolution(reso: String): Int = {
//    reso match {
//      case "1280x800" => 0
//      case "1366x768" => 1
//      case "1440x1024" => 2
//      case "1600x900" => 3
//      case "1600x1024" => 4
//      case "1680x1050" => 5
//      case "1920x1080" => 6
//      case "1920x1200" => 7
//      case "2048x1200" => 8
//      case "2880x1920" => 9
//      case "3200x1800" => 10
//      case "3840x2160" => 11
//      case _ => 12
//    }
//  }
//
//  def eduL(edu: Int): String = {
//    edu match {
//      case  0 => "高中、中专及以下"
//      case  1 => "专科、本科"
//      case  2 => "硕士"
//      case  3 => "硕士以上"
//      case  _ => "专科、本科"
//    }
//  }
//
//  def salaryL(salary: Int): String = {
//    salary match {
//      case  0 => "3500以下"
//      case  1 => "3500-8000"
//      case  2 => "8000-10000"
//      case  3 => "10000-15000"
//      case  4 => "15000以上"
//      case _ => "8000-10000"
//    }
//  }
//
//
//}
