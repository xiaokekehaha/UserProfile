package com.lenovo.persona.analysis

import org.apache.spark.sql.SparkSession

object Province {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Country_Analysis")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext

    val data = spark.sql("select sid,province from proj_gucp_dw_beta1.testb").map(x => {
      val sid = x.getAs[String](0)
      val province = x.getAs[String](1)
      (sid, province)
    }).filter(x => x._2 != "nop").rdd.groupByKey().map(x => {
      val sid = x._1
      val prov = x._2.head
      (prov, 1)
    }).reduceByKey(_ + _).toDF("province", "count")

    data.createOrReplaceTempView("province")
    spark.sql("create table proj_gucp_dw_beta1.province_anas as select * from province")


  }


}
