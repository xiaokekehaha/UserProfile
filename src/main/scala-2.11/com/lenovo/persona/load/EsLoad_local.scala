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
//object EsLoad_local {
//
//  def main(args: Array[String]): Unit = {
//
//    val ES_LOC_INDEX = "gucp_test"
//    val ES_LOC_TYPE = "contactinfo"
//
//    val spark = SparkSession
//      .builder
//      .appName(s"${this.getClass.getSimpleName}")
//      .getOrCreate()
//
//    import spark.implicits._
//
//    val data = spark.createDataset(Seq(1,2,3,4,5,6,7))
//
//    val json = "{" +
//      "\"user\":\"kimchy\"," +
//      "\"postDate\":\"2013-01-30\"," +
//      "\"message\":\"trying out Elasticsearch\"" +
//      "}"
//    val s1 = System.currentTimeMillis()
//    data.foreachPartition(x => {
//      val settings = Settings.settingsBuilder()
//        .put("cluster.name", "mycluster").build()
//      val client = TransportClient.builder().settings(settings).build()
//        .addTransportAddress(
//          new InetSocketTransportAddress(InetAddress.getByName("10.120.193.9"), 9300))
//      x.foreach(msg => {
//        val response = client.prepareIndex(ES_LOC_INDEX, ES_LOC_TYPE, msg+"").setSource(json).get()
//      })
//      client.close()
//      println("Time of 10000 datas:" + (System.currentTimeMillis() - s1))
//    })
//  }
//}