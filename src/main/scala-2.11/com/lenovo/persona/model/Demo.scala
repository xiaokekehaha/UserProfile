package com.lenovo.persona.model

// scalastyle:off
import com.lenovo.persona.utils.ConfigUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

object Demo {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("asd")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)

    import hiveContext.sql

    sql("use d_lucp_dw")

    val a = "testttttt"
    sql(s"insert into log select '$a' from dual")



  }
}
