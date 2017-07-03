
// scalastyle:off
package com.lenovo.persona.model

import com.lenovo.persona.utils.ConfigUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd


//    spark.sql("INSERT OVERWRITE table d_lucp_dw.hhl_idclean_dmplenovoid SELECT distinct lenovoid y, x, case when x rlike '^1[3|4|5|7|8]\\d{9}$' then '12'      when x rlike '^([a-zA-Z0-9_\\.\\-])+\\@(([a-zA-Z0-9\\-])+\\.)+([a-zA-Z0-9]{2,4})+$'  then '13'                   else '' end type from d_lucp_dw.tmp_it_lenovoid_result where x rlike '^1[3|4|5|7|8]\\d{9}$' or x rlike '^([a-zA-Z0-9_\\.\\-])+\\@(([a-zA-Z0-9\\-])+\\.)+([a-zA-Z0-9]{2,4})+$'")


object Up3 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val filter_cookie = spark.sql("select x from d_lucp_dw.hhl_fx_count20").map( x => x.getAs[String]("x")+5).rdd.collect()

    val sc = spark.sparkContext
    val data = spark.sql("" +
      "select y, x,type from d_lucp_dw.hhl_fx1").map(x => {
      val v1 = x.getAs[String]("y")
      val v2 = x.getAs[String]("x")
      val typed = x.get(2).toString.split("|")
      (v1+""+typed(0), v2+""+typed(1))
    }).filter(x => {
      if (filter_cookie.contains(x._1) || filter_cookie.contains(x._2))
        false
      else
        true
    })

    val dic = data.flatMap(x => List(x._1, x._2)).distinct().rdd.zipWithUniqueId().toDS().cache()
    val rowEdges_1 = data.joinWith(dic, dic("_1") === data("_1")).map(x => (x._2._2, x._1._2))
    val rowEdges = rowEdges_1.joinWith(dic, dic("_1") === rowEdges_1("_2")).map(x => (x._1._1, x._2._2)).rdd
    val graph = Graph.fromEdgeTuples(rowEdges, 1)
    val cc = graph.connectedComponents().vertices
    val tmp = cc.map(x => (x._1.toLong, x._2.toLong)).toDS()
    val ccByUsername = dic.rdd.map(_.swap).join(cc).map {
      case (id, (username, cc)) => (username, cc.toLong)
    }.toDS()

    val rr = ccByUsername.map(x => (x._2.toLong, low(x._1))).map(x => ("c_" + x._1, x._2.substring(0,x._2.size-1),x._2.substring(x._2.size-1))).toDF("sid", "id","idtype").registerTempTable("table1")
    spark.sql("create table d_lucp_dw.hhl_fx_result as select * from table1")
  }

  def low(str: String): String = {
    str match {
      case str if str.contains(":3") => str.toLowerCase()
      case _ => str
    }
  }
}