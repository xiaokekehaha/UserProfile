//package com.lenovo.persona.model.labelrdd
//
//// scalastyle:off
//
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.hive.HiveContext
//
//object Classify {
//
//  // 选用混合模型，使用选举器，票数多的为最终结果
//  def main(args: Array[String]): Unit = {
//
////    val sparkConf = new SparkConf().setAppName("Fst_Step_Graph_Components")
////    val sc = new SparkContext(sparkConf)
////    val hiveContext = new HiveContext(sc)
//    //    val sqlContext = new SQLContext(sc)
////    import hiveContext.sql
//
//
//    val spark = SparkSession.builder().appName("asd").master("local").getOrCreate()
//    val sc = spark.sparkContext
//
//
//    val pretreatment = new Pretreatment
//    val nbModel = new NBTrain
//    val trainData = pretreatment.process(sc)
//    val model = nbModel.process(trainData)
//    // 测试数据格式未知
//    model.save(sc,"/Users/liziyao/Desktop/NBModel_2")
//
//
//
//
//  }
//
//
//}
