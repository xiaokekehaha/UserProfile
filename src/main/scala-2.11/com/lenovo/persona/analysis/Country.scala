package com.lenovo.persona.analysis

// scalastyle:off

import org.apache.spark.sql.SparkSession
import com.lenovo.persona.utils.DatasetExe._

object Country {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Country_Analysis")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext

    // 国家代码
    val countryCode_1 = spark.sql("select tagcodevalue,tagcode from proj_gucp_dw.mb_tag where l_table = 'countrycode'").map(x => {
      val tagcode = x.getAs[String](0)
      val tagvalue = x.getAs[String](1)
      (tagcode, tagvalue)
    })

    val third_Country = spark.sql("select srccode,codevalue from proj_gucp_dw.mb_area where l_table = 'nation'").map(x => {
      val tagcode = x.getAs[String](0)
      val tagvalue = x.getAs[String](1)
      (tagcode, tagvalue)
    })

    val countryCode = countryCode_1.union(third_Country).distinct().collectAsMap()

    // 省份代码
    val provinceCode_1 = spark.sql("select tagcodevalue,tagcode from proj_gucp_dw.mb_tag where l_table = 'provincecode'").map(x => {
      val tagcode = x.getAs[String](0)
      val tagvalue = x.getAs[String](1)
      (tagcode, tagvalue)
    })

    val third_province = spark.sql("select srccode,codevalue from proj_gucp_dw.mb_area where l_table = 'province'").map(x => {
      val tagcode = x.getAs[String](0)
      val tagvalue = x.getAs[String](1).split("-")(1)
      val c = x.getAs[String](1).split("-")(0)
      (tagcode,c , tagvalue)
    }).filter( x => x._2 == "中国").map( x => (x._1,x._3))

    val provinceCode = provinceCode_1.union(third_province).distinct().collectAsMap()


    // 广播
    val cCMap = sc.broadcast(countryCode)
    val pCMap = sc.broadcast(provinceCode)

    val data = spark.sql("select * from proj_gucp_dw.lzy_cpp")






      //    val data = spark.sql("select sid,tag from proj_gucp_dw.dj_count_area where tag is not null and tag <> ''")
      .map(x => {
      val sid = x.getAs[String](0)
      val tag = x.getAs[String](1).split(":")(1)
      val cMap = cCMap.value
      val pMap = pCMap.value
      val country = cMap.getOrElse(tag, "noc")
      val province = pMap.getOrElse(tag, "nop")
      (sid, country, province)
    })

//    val a = data.toDF("sid", "country", "province")
//    a.createOrReplaceTempView("a")
//    spark.sql("create table proj_gucp_dw_beta1.testb as select * from a")

    val cData = data.map(x => (x._1, x._2)).filter(x => x._2 != "noc").rdd.groupByKey().map(x => {
      val sid = x._1
      val countr = x._2.head
      (countr, 1)
    }).reduceByKey(_ + _).toDF("country", "count")

    val pData = data.map(x => (x._1, x._3)).filter(x => x._2 != "nop").rdd.groupByKey().map(x => {
      val sid = x._1
      val prov = x._2.head
      (prov, 1)
    }).reduceByKey(_ + _).toDF("province", "count")

    cData.createOrReplaceTempView("country")
    pData.createOrReplaceTempView("province")

    spark.sql("create table proj_gucp_dw_beta1.country_ana_a as select * from country")
    spark.sql("create table proj_gucp_dw_beta1.province_ana_a as select * from province")
  }

}
