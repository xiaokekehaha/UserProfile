/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off

// Todo: 需要将结果json化

package com.lenovo.persona.model

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.{Row, SQLContext}
import org.paukov.combinatorics.Factory
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConverters._

object Up2Hive {

  case class Relation(super_id:String,relation:String)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    // Creates a SparkSession.
//    val spark = SparkSession
//      .builder
//      .appName(s"${this.getClass.getSimpleName}")
//      .config("spark.sql.warehouse.dir", "/user/hive/071/warehouse")
//      .enableHiveSupport()
//      .getOrCreate()

//    import spark.implicits._
//
//    val sc = spark.sparkContext

    println("1")

    val sparkConf = new SparkConf().setAppName("item_item")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    val sqlContext=new SQLContext(sc)
    import hiveContext.sql


    println("2")
    sql("use d_lucp_dw")

    println(3)


//    val hiveData = sql("select lenovoid,cs_customerid,microblogid,wechatid,mobile_md5,email,unique_cookie,imei_num from lenovo_user_gain_data_rel").map {
//      case Row(lenovoid: String, cs_customerid: String, microblogid: String
//      , wechatid: String, mobile_md5: String, email: String, unique_cookie: String
//      , imei_num: String) => (lenovoid, cs_customerid, microblogid, wechatid, mobile_md5, email, unique_cookie, imei_num)
//    }.rdd


//    val hiveData = sql("select lenovoid,cs_customerid,microblogid,wechatid,mobile_md5,email,unique_cookie,imei_num from lenovo_user_gain_data_rel").rdd.map( x => {
//      val lenovoid = x.getAs[String]("lenovoid")
//      val cs_customerid = x.getAs[String]("cs_customerid")
//      val microblogid = x.getAs[String]("microblogid")
//      val wechatid = x.getAs[String]("wechatid")
//      val mobile_md5 = x.getAs[String]("mobile_md5")
//      val email = x.getAs[String]("email")
//      val unique_cookie = x.getAs[String]("unique_cookie")
//      val imei_num = x.getAs[String]("imei_num")
//      (lenovoid,cs_customerid,microblogid,wechatid,mobile_md5,email,unique_cookie,imei_num)
//    })

    val dtd = args(0)
    val src = args(1)
    val hiveData = sc.textFile(s"/user/hive/d_lucp_dw.db/user_relation")


    println("number of source:" + hiveData.count())

    val data = hiveData
      .flatMap(x => {
        val itr = List("lenovoid:" + x._1, "cs_customerid:" + x._2, "microblogid:" + x._3,
          "wechatid:" + x._4, "mobile_md5:" + x._5, "email:" + x._6, "unique_cookie:" + x._7, "imei_num:" + x._8)
        val initialVector = Factory.createVector(itr.asJava)
        var index = 2
        if (itr.length < 2) {
          index = itr.length
        }
        val result = Factory.createSimpleCombinationGenerator(initialVector, index)
        result.generateAllObjects().asScala.toList.map {
          item =>
            item.asScala.toList.sorted
        }.map(x => (x(0), x(1)))
      }).filter(x => {
      if (x._1.last == ':' || x._2.last == ':')
        false
      else
        true
    })

    println(data.count())
//    data.saveAsTextFile("lenovorelation")


    // (customid:120141101001791,1)
    val dic = data.flatMap(x => List(x._1, x._2)).zipWithUniqueId()

    val rowEdges = data.join(dic).map(_._2).join(dic).map(_._2)

    val graph = Graph.fromEdgeTuples(rowEdges, 1)

    val cc = graph.connectedComponents().vertices

    val ccByUsername = dic.map(_.swap).join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }

    val rr = ccByUsername.map(x => (x._2, x._1)).groupByKey().map(x => {
      val id = x._1
      val mes = x._2.toList.distinct
      (id, toJson(mes))
    }).map( x => Row(x._1.toString,x._2))

    val structType=StructType(Array(
      StructField("super_id",StringType,true),
      StructField("relation",StringType,true)
    ))

    println("the number of result:"+rr.count())

    val a = sqlContext.createDataFrame(rr,structType)

    a.createOrReplaceTempView("relation")

//    rr.toDF().createOrReplaceTempView("relation")
    sql("insert into super_v1 select * from relation")



    // Todo: Save???


  }

  def toJson(list: List[String]): String = {

    val groupList = list.map(x => {
      val tup = x.split(":")
      val id = tup(0)
      val value = tup(1)
      (id, value)
    }).groupBy(_._1).map(x => {
      val key = x._1
      val value = x._2.map(_._2)
      (key, value)
    })

    val lenovoid = groupList.getOrElse("lenovoid", List(""))
    val cs_customerid = groupList.getOrElse("cs_customerid", List(""))
    val microblogid = groupList.getOrElse("microblogid", List(""))
    val wechatid = groupList.getOrElse("wechatid", List(""))
    val mobile_md5 = groupList.getOrElse("mobile_md5", List(""))
    val email = groupList.getOrElse("email", List(""))
    val unique_cookie = groupList.getOrElse("unique_cookie", List(""))
    val imei_num = groupList.getOrElse("imei_num", List(""))

    val json =
      (
        ("lenovoid" -> lenovoid) ~
          ("cs_customerid" -> cs_customerid) ~
          ("microblogid" -> microblogid) ~
          ("wechatid" -> wechatid) ~
          ("mobile_md5" -> mobile_md5) ~
          ("email" -> email) ~
          ("unique_cookie" -> unique_cookie) ~
          ("imei_num" -> imei_num)
        )
    compact(render(json))
  }

}
