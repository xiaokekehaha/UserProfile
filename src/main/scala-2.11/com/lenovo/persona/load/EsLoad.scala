//package com.lenovo.persona.load
//
//// scalastyle:off
//import java.net.InetAddress
//
//import org.apache.spark.sql.SparkSession
//import org.elasticsearch.client.transport.TransportClient
//import org.elasticsearch.common.settings.Settings
//import org.elasticsearch.common.transport.InetSocketTransportAddress
//
//object EsLoad {
//
//  def main(args: Array[String]): Unit = {
//
//    val ES_LOC_INDEX = "gucp_test_620"
//    val ES_LOC_TYPE = "contactinfo"
//
//    val spark = SparkSession
//      .builder
//      .appName(s"${this.getClass.getSimpleName}")
//      .enableHiveSupport()
//      .getOrCreate()
//
//    import spark.implicits._
//
//    val data = spark.sql("select sid,tagjosn from d_lucp_dw.tag_hhl").map(x => (x.get(0).toString, x.get(1).toString))
//    data.foreachPartition(x => {
//      val settings = Settings.settingsBuilder()
//        .put("cluster.name", "mycluster").build()
//      val client = TransportClient.builder().settings(settings).build()
//        .addTransportAddress(
//          new InetSocketTransportAddress(InetAddress.getByName("10.120.193.9"), 9300))
//      x.foreach(msg => {
//        val response = client.prepareIndex(ES_LOC_INDEX, ES_LOC_TYPE, msg._1).setSource(msg._2).get()
//      })
//      client.close()
//    })
//  }
//}