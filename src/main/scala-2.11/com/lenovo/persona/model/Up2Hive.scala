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
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.{Row, SparkSession}
import org.paukov.combinatorics.Factory

import scala.collection.JavaConverters._

object Up2Hive {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .config("spark.sql.warehouse.dir", "/user/hive/071/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    spark.sql("use databaseName")

    val hiveData = spark.sql("select lenovoid,cs_customerid,microblogid,wechatid,mobile_md5,email,unique_cookie,imei_num from lenovo_user_gain_data_rel").map {
      case Row(lenovoid: String, cs_customerid: String, microblogid: String
      , wechatid: String, mobile_md5: String, email: String, unique_cookie: String
      , imei_num) => (lenovoid, cs_customerid, microblogid, wechatid, mobile_md5, email, unique_cookie, imei_num)
    }.rdd

    val data = hiveData
      .flatMap(x => {
        val itr = List("lenovoid:" + x._1, "customid:" + x._2, "microblogid:" + x._3,
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

    //    println(data.collect.mkString("\n"))

    val dic = data.flatMap(x => List(x._1, x._2)).zipWithUniqueId()

    val rowEdges = data.join(dic).map(_._2).join(dic).map(_._2)

    //    val rowEdges = tmp.map( x => (x._2._1,x._1))
    //      .groupByKey()
    //      .map( x => {
    //        val itr = x._2.toList
    //        if(itr.size == 1){
    //          (itr(0).asInstanceOf[VertexId],itr(0).asInstanceOf[VertexId])
    //        } else {
    //          (itr(0).asInstanceOf[VertexId],itr(1).asInstanceOf[VertexId])
    //        }
    //      })
    //
    val graph = Graph.fromEdgeTuples(rowEdges, 1)
    //
    val cc = graph.connectedComponents().vertices

    val ccByUsername = dic.map(_.swap).join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }
    //
    val rr = ccByUsername.map(x => (x._2, x._1)).groupByKey().map(x => {

      val id = x._1
      val mes = x._2.toList.distinct.mkString("|")
      val sortKey = x._2.size

      (sortKey, (id, mes))
    }).sortByKey(false)

    // scalastyle:off
    println(rr.map(_._2).collect().mkString("\n"))


  }

}
