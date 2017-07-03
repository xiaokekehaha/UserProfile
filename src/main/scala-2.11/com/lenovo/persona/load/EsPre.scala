package com.lenovo.persona.load

// scalastyle:off
import org.apache.spark.sql.SparkSession
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}

object EsPre {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._


    val data = spark.sql("select sid,tag from d_lucp_dw.gucp_tag_rel").map(x => (x.get(0).toString ,x.get(1).toString)).map( x => {
      val indexJson ="index" -> ("_id" -> x._1)
      compact(render(indexJson))+"\n" + x._2
    }).rdd.repartition(600)
    data.saveAsTextFile("es_600")
  }
}