package com.lenovo.persona.hbase

// scalastyle:off

import java.util.Date

import com.lenovo.persona.utils.ConfigUtils
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat

import scala.reflect.ClassTag

object HFileCreate {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("HFileCreate")
//      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val conf = HBaseConfiguration.create()

    val tableName = "test_v1"
    val columnFamilyName = "tag"
    val date = new Date().getTime

//    val hiveData = spark.sql("select * from d_lucp_dw.mid_user_profile limit 100")
//      .map(x => {
//        val id = x.getAs[String]("id")
//        val idtype = x.getAs[String]("idtype")
//        val tag = x.getAs[String]("tag")
//        val l_date = x.getAs[String]("l_date")
//        val src = x.getAs[String]("src")
//        (id, idtype, tag, l_date, src)
//      })

    val hiveData = spark.createDataset(List(("a","b","c","d","e")))

    val hfileData = hiveData.map(x => {
      val rowKey = bys(x._1 + "__" + x._2 + "__" + x._5 + "__" + x._4)
      val column = bys("data")
      val value = bys(x._3)
      val family = bys(columnFamilyName)
      (rowKey)
//      (new ImmutableBytesWritable(rowKey),new KeyValue(rowKey,family,column,date,value))
    }).rdd
//    hfileData.saveAsNewAPIHadoopFile("hfile_v1",
//      classOf[ImmutableBytesWritable],
//      classOf[KeyValue],
//      classOf[HFileOutputFormat],
//      conf
//    )
    ConfigUtils.overwriteTextFile("asd",hfileData)
  }

  def bys(a: String) = Bytes.toBytes(a)


}
