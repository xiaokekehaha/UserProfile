package com.lenovo.persona.model


// scalastyle:off
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object Step2 {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Fst_Step_Graph_Components")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    //    val sqlContext = new SQLContext(sc)
    import hiveContext.sql

    sql("use d_lucp_dw")


    val hiveData = sql("select lenovoid,cs_customerid,microblogid,wechatid,mobile_md5,email,unique_cookie,imei_num,lps_did from user_relation where src = 'dmp'").rdd.map(x => {
      val lenovoid = x.getAs[String]("lenovoid")
      val cs_customerid = x.getAs[String]("cs_customerid")
      val microblogid = x.getAs[String]("microblogid")
      val wechatid = x.getAs[String]("wechatid")
      val mobile_md5 = x.getAs[String]("mobile_md5")
      val email = x.getAs[String]("email")
      val unique_cookie = x.getAs[String]("unique_cookie")
      val imei_num = x.getAs[String]("imei_num")
      val lps_did = x.getAs[String]("lps_did")
      (lenovoid, cs_customerid, microblogid, wechatid, mobile_md5, email, unique_cookie, imei_num, lps_did)
    })


    val aData = sql("").rdd.map( x => {
      val lenovoid = x.getAs[String]("lenovoid")
      val superid = x.getAs[String]("superid")
      val json = x.getAs[String]("json")
      (lenovoid,(superid,json))
    })


  }

}
