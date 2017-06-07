
// scalastyle:off
package com.lenovo.persona.model

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.rdd


object Up2 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._


    val data = spark.sql("" +
      "select concat(y,':',substr(type,1,1)) y,concat(x,':',substr(type,2,1)) x from d_lucp_dw.hhl_clean_distinct limit 100000000")
      .rdd.map( x => {
      val v1 = x.getAs[String]("y")
      val v2 = x.getAs[String]("x")
      (v1,v2)
    })


    val dic = data.flatMap( x => List(x._1,x._2)).zipWithUniqueId().toDS()

    val datads = data.toDS()
    val rowEdges_1 = datads.joinWith(dic,dic("_1") === datads("_1")).map( x => (x._2._2,x._1._2))
    val rowEdges = rowEdges_1.joinWith(dic,dic("_1")===rowEdges_1("_2")).map( x =>(x._1._1,x._2._2) ).rdd


//      .join(dic)

//      .map(_._2).join(dic).map(_._2)


//
    val graph = Graph.fromEdgeTuples(rowEdges, 1)
//
    val cc = graph.connectedComponents().vertices

    val tmp = cc.map( x => (x._1.toLong,x._2.toLong)).toDS()
//    val xx = tmp.joinWith(dic,dic("mes") === datads("v1")).map( x => (x._2._2,x._1._2))
    val tt = tmp.joinWith(dic,dic("_2")===tmp("_1")).map( x => x._1._2+"\005"+x._2._1)

    tt.write.save("user_graph_mid")



      //.map( x => x._1 +"\005" + x._2)
//      .map(x => Row(x._1.toString, x._2.toString))

    //    sql(s"insert into log select 'step124' from dual")
//    val structType = StructType(Array(
//      StructField("super_id", StringType, true),
//      StructField("relation", StringType, true)
//    ))
//
//
//    val a = hiveContext.createDataFrame(tt,structType)
//
//    a.registerTempTable("idres")
//     sql("insert into super_v1 select * from idres")

//    tt.saveAsTextFile("/user/u_lucp_dw/user_graph_mid")





  }

}
