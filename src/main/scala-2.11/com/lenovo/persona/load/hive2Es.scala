package com.lenovo.persona.load

// scalastyle:off
import org.apache.spark.sql.SparkSession
//import toES.EsClient
import toES.ESRestUtil
import io.searchbox.core.Index

object hive2Es {

  def main(args: Array[String]): Unit = {
    //
    val ES_LOC_INDEX = "gucp"
    val ES_LOC_TYPE = "userprofile"

    //    val ES_LOC_INDEX = "gucp"
    //    val ES_LOC_TYPE = ""


    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val path = args(0)

    val data = spark.sql("select sid,tagjosn from d_lucp_dw.tag_hhl").map(x => (x.get(0).toString ,x.get(1).toString))
//    data.saveAsTextFile("result")
//    val part1 = spark.sparkContext.textFile("result/part-00000").map( x => {
//      val line = x.split("\001")
//      val sid = line(0)
//      val tagjson = line(1)
//      (sid,tagjson)
//    })




    println("**********Partition numbers:"+data.rdd.partitions.length)
    data.foreachPartition(x => {
      val client = ESRestUtil.getJestClient
//      x.foreach(msg => {
//        var index = new Index.Builder(msg._2).index(ES_LOC_INDEX).`type`(ES_LOC_TYPE).id(msg._1).build()
//        client.execute(index)
//      })
      var n = 0
      for( i <- 0 until x.size) {
        if( n == 10000 || i == x.size-1){
          ???
          n = 0
        } else {
          // tianjia
          n += 1
        }
      }






    })
  }
}