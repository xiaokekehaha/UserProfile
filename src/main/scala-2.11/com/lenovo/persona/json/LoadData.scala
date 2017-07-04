package com.lenovo.persona.json

// scalastyle:off

import java.util.Properties

import com.lenovo.persona.utils.ConfigUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadData {

  lazy val url = "jdbc:mysql://10.120.193.32:3306/LUDP"
  lazy val username = "dujie"
  lazy val password = "dujie"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Load-Mysql-Data").getOrCreate()
    val hr_cleaned_source = loadFromTable(spark, "label_info").rdd.map(x => x.getAs[String]("HR_PAGE_CONTENT")).filter(EnFilter)
    ConfigUtils.overwriteTextFile("data/hr_cleaned_source", hr_cleaned_source)

  }

  def loadFromTable(ss: SparkSession, tableName: String): DataFrame = {
    val sqlContext = ss.sqlContext
    val uri = s"$url?user=$username&password=$password&useUnicode=true&characterEncoding=UTF-8"
    val prop = new Properties()
    prop.put("driver", "com.mysql.jdbc.Driver")
    val df: DataFrame = sqlContext.read.jdbc(uri, tableName, prop)
    df
  }

  def EnFilter(line: String): Boolean = {

    val regular = """^[A-Za-z]+$""".r
    var count = 0
    line.split(" ").foreach { x => {
      val tmp = regular.findFirstIn(x).getOrElse("000")
      if (tmp != "000") count += 1
    }
    }
    if (count >= 100)
      false
    else
      true
  }

}
