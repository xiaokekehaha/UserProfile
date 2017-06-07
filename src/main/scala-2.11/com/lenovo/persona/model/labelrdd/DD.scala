package com.lenovo.persona.model.labelrdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object DD {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("NBM")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    //    val sqlContext = new SQLContext(sc)
    import hiveContext.sql

    val data = sql("select * from d_dmp.service_model_device_swtype").repartition(15)


    val structType = StructType(Array(
      StructField("lps_did", StringType, true),
      StructField("os_version", StringType, true),
      StructField("language", StringType, true),
      StructField("resolution", StringType, true),
      StructField("city", StringType, true),
      StructField("software_category", StringType, true)
    ))

    //    val a = hiveContext.createDataFrame(predictionAndLabel,structType)
    //    a.registerTempTable("wasd")
    //    sql("insert into d_dmp.service_device_model select * from wasd")

//    val a = hiveContext.createDataFrame(data, structType)

    data.registerTempTable("wasd")
    sql("insert into d_dmp.hhl_sdm select * from wasd")

  }

}
