package com.lenovo.persona.load

//scalastyle:off

import io.searchbox.core.{Bulk, Index}
import org.apache.spark.sql.SparkSession
import toES.ESRestUtil


object Hive2EsBulk600 {

  def main(args: Array[String]): Unit = {
    //
    //val ES_LOC_INDEX = "profile4"
    val ES_LOC_INDEX = "profile"
    val ES_LOC_TYPE = "userprofile"

    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      //.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    //数据重分布
//
//  val data = spark.sql("select sid,tag from d_lucp_dw.gucp_tag_rel").map(x => (x.get(0).toString+"|"+ x.get(1).toString)).rdd.repartition(20)
//
//    data.saveAsTextFile("hhltag")
//
//    val part1 = spark.sparkContext.textFile("hhltag/part-00001").map( x => {
//      val line = x.split("\\|")
//      val sid = line(0)
//      val tagjson = line(1)
//      (sid,tagjson)
//    }).repartition(3000)
val data = spark.sql("select a.sid,a.tag from d_lucp_dw.gucp_tag_rel a").map(x => (x.get(0).toString, x.get(1).toString)).repartition(40000).cache()
    

    data.foreachPartition(f = x => {
      val client = ESRestUtil.getJestClient
      val bulk = new Bulk.Builder().defaultIndex(ES_LOC_INDEX).defaultType(ES_LOC_TYPE)

      x.foreach( msg => {
        //val index = new Index.Builder(msg._2).id(msg._1).build()
        val index = new Index.Builder(msg._2).build()

        // 如果不指定id，es 会自增维护ID
        println(msg._1)
        bulk.addAction(index)
      }
      )

      client.execute(bulk.build())

   })
  }

}