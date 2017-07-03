package com.lenovo.persona.model

// scalastyle:off
import java.io.StringReader
import java.text.SimpleDateFormat

import com.google.gson.stream.JsonReader
import com.google.gson.{Gson, GsonBuilder}
import io.searchbox.client.JestClientFactory
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, Index}
import org.apache.spark.sql.SparkSession
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}

object TraceTest {


  def main(args: Array[String]): Unit = {

    val tableName = "all_b"

    val ES_LOC_INDEX = "testtt"
    val ES_LOC_TYPE = "uutt"

    val spark = SparkSession
      .builder()
      .appName("User_Trace")
      .enableHiveSupport()
      .getOrCreate()

    val a = ("a", "b", "c", "d", "e")
    val msg = toJson(a)

    val gson = new Gson()
    val reader = new JsonReader(new StringReader(""))
    reader.setLenient(true)

    val factory = new JestClientFactory()
    factory.setHttpClientConfig(new HttpClientConfig
    //                .Builder("http://10.100.124.226:9222")

    .Builder("http://10.120.193.9:9222")
      .gson(new GsonBuilder().setLenient().setDateFormat("yyyy-MM-dd'T'HH:mm:ss").create())
      .multiThreaded(true)
      .readTimeout(10000000)
      .build())
    val client = factory.getObject()
    val bulk = new Bulk.Builder().defaultIndex(ES_LOC_INDEX).defaultType(ES_LOC_TYPE)
    val index = new Index.Builder(msg).build()
    bulk.addAction(index)
    client.execute(bulk.build())
  }

  def toJson(src: (String, String, String, String, String)): String = {

    //    val js = (
    //      ("index" -> ("_id" -> src._1))
    //    )

    val json =
      ("superid" -> src._1) ~
        ("date" -> src._2) ~
        ("theme" -> src._3) ~
        ("content" -> src._4) ~
        ("event" -> src._5)
    compact(render(json))
  }

  def dateTrans(src: String): String = {
    val spdf = new SimpleDateFormat("yyyyMMdd")
    val date = spdf.parse(src)
    val spdf2 = new SimpleDateFormat("yyyy-MM-dd")
    spdf2.format(date)
  }

}