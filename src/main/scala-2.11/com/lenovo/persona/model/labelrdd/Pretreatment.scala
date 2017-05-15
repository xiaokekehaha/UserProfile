// scalastyle:off
//package com.lenovo.persona.model.labelrdd
//
//import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.mllib.regression.LabeledPoint
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{Dataset, Row, SparkSession}
//
//// scalastyle:off
//// change data to Labelpoint
//class Pretreatment {
//
//  lazy val databaseName = ""
//  lazy val tableName = ""
//
//  def process(spark: SparkSession): RDD[LabeledPoint] = {
//
//    import spark.implicits._
//    spark.sql(s"use $databaseName")
//
//    val data = spark.sql(s"select ips_did,gender,soft1,soft2,industry,age,device from $tableName")
//      .map {
//        case Row(ips_did: String, gender: Double, soft1: Double, soft2: Double, label: Double) =>
//          ((gender, soft1, soft2), label)
//      }.rdd
//
//    // 保存ips_did 的码表数据
//    val parsed = data.map( x => {
//      val features = x._1.productIterator.toArray.map(_.asInstanceOf[Double])
//      val label = x._2
//      val vector = Vectors.sparse(3,Array(1,2,3),features)
//      new LabeledPoint(label,vector)
//    })
//    parsed
//  }
//
//}
