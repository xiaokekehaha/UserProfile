package com.lenovo.persona.model.labelrdd


// scalastyle:off
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SoftTime {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("NBM")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    //    val sqlContext = new SQLContext(sc)
    import hiveContext.sql

    val 


  }

}
