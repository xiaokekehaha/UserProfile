// scalastyle:off
package com.lenovo.persona.model.labelrdd

import com.lenovo.persona.utils.ConfigUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

// scalastyle:off
// change data to Labelpoint
class Pretreatment extends Serializable{

  lazy val databaseName = ""
  lazy val tableName = ""

  def process(sc: SparkContext): RDD[LabeledPoint] = {

    //    spark.sql(s"use $databaseName")

    val data = sc.textFile("/Users/liziyao/Desktop/trainSala.txt")
      .map(_.split(","))
        .filter( x => x.size == 6)
      .map(x => {
        val os_version = osVersion(x(0))
        val lang = langT(x(1))
        val reso = resolution(x(2))
        val city = cityCode(x(3))
        val softs = x(4).split("-").map(typeCode)
        val edu = x(5).toDouble
        var t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15, t16, t17, t18, t19, t20, t21, t22, t23 = 0
        softs.map(x => x match {
          case 1 => t1 = 1
          case 2 => t2 = 1
          case 3 => t3 = 1
          case 4 => t4 = 1
          case 5 => t5 = 1
          case 6 => t6 = 1
          case 7 => t7 = 1
          case 8 => t8 = 1
          case 9 => t9 = 1
          case 10 => t10 = 1
          case 11 => t11 = 1
          case 12 => t12 = 1
          case 13 => t13 = 1
          case 14 => t14 = 1
          case 15 => t15 = 1
          case 16 => t16 = 1
          case 17 => t17 = 1
          case 18 => t18 = 1
          case 19 => t19 = 1
          case 20 => t20 = 1
          case 21 => t21 = 1
          case 22 => t22 = 1
          case 23 => t23 = 1
        })
        (List(os_version, lang, reso, city, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15, t16, t17, t18, t19, t20, t21, t22,t23),edu)
      })


    // 保存ips_did 的码表数据
    val parsed = data.map(x => {
      val features = x._1.toArray.map(_.asInstanceOf[Double])
      val label = x._2
      val vector = Vectors.dense(features)
      new LabeledPoint(label, vector)
    })
    parsed
  }

  def cityCode(city: String): Int = {
    val cityMap = ConfigUtils.getConfig("/city.properties")
    val code = cityMap.getOrElse(city, 7)
    code.toString.toInt
  }

  def typeCode(typed: String): Int = {
    val typeMap = ConfigUtils.getConfig("/pc_soft_type.properties")
    val code = typeMap.getOrElse(typed, 23)
    code.toString.toInt
  }

  def osVersion(osV: String): Int = {
    osV match {
      case "Win 10.0" => 0
      case "Win 6.3" => 1
      case _ => 2
    }
  }

  def langT(lang: String): Int = {
    lang match {
      case "zh" => 0
      case "en" => 1
      case "ko" => 2
      case _ => 3
    }
  }

  def resolution(reso: String): Int = {
    reso match {
      case "1280x800" => 0
      case "1366x768" => 1
      case "1440x1024" => 2
      case "1600x900" => 3
      case "1600x1024" => 4
      case "1680x1050" => 5
      case "1920x1080" => 6
      case "1920x1200" => 7
      case "2048x1200" => 8
      case "2880x1920" => 9
      case "3200x1800" => 10
      case "3840x2160" => 11
      case _ => 12
    }
  }

  def genderL(gender: String): Int = {
    gender match {
      case "男" => 1
      case "女" => 0
    }
  }

  def ageL(age: String): Int = {
    age match {
      case "19-24" => 0
      case "25-34" => 1
      case "35-49" => 2
      case _ => 3
    }
  }

  def eduL(edu: String): Int = {
    edu match {
      case "高中、中专及以下" => 0
      case "专科、本科" => 1
      case "硕士" => 2
      case "硕士以上" => 3
    }
  }

  def salaryL(salary: String): Int = {
    salary match {
      case "3500以下" => 0
      case "3500-8000" => 1
      case "8000-10000" => 2
      case "10000-15000" => 3
      case "15000以上" => 4
    }
  }
}
