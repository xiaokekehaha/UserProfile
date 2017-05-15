package com.lenovo.persona.utils

import java.io.FileInputStream
import java.util.Properties
// import org.apache.commons.io.IOUtils
// import org.apache.hadoop.fs.Path
// import org.apache.spark.SparkContext
// import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters.propertiesAsScalaMapConverter

/*
 * this object is a toolbox,add some common def at here
 * */
object ConfigUtils {

  /**
    * 根据传入参数返回其在properties配置文件中的value值
    *
    * @param prop 配置文件对象
    * @param strs 配置文件中的key
    * @return value对应的数组
    */
  def getProps(prop: collection.Map[String, String], strs: String*): Seq[String] = {
    strs.map { x =>
      val value = prop.getOrElse(x, "")
      if ("" == value) throw new Exception(s"key:$x does not exist!")
      value
    }
  }

  // get property's message from resources folder
  def getConfig(path: String): scala.collection.mutable.Map[String, String] = {
    val prop = new Properties()
    val inputStream = getClass().getResourceAsStream(path)
    try {
      prop.load(inputStream)
      propertiesAsScalaMapConverter(prop).asScala
    } finally inputStream.close()
  }

  // get property's message from local folder
  def getConfig2(path: String): scala.collection.mutable.Map[String, String] = {
    val prop = new Properties()
    val in = new FileInputStream(path)
    try {
      prop.load(in)
      propertiesAsScalaMapConverter(prop).asScala
    } finally in.close()
  }

  //  def deletePath(sc: SparkContext, path: String): Unit = {
  //    val hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
  //    val hdfsPath = new org.apache.hadoop.fs.Path(path)
  //    if (hdfs.exists(hdfsPath))
  //      hdfs.delete(hdfsPath, true)
  //  }
  //
  //  def overwriteTextFile[T](path: String, rdd: RDD[T]): Unit = {
  //    deletePath(rdd.context, path)
  //    rdd.saveAsTextFile(path)
  //  }
  //
  //  def writeSingleHFile(sc:SparkContext,inpath:String,outpath:String):Unit = {
  //    val hdfs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
  //    val a = hdfs.open(new Path(inpath))
  //    val b = hdfs.append(new Path(outpath))
  //    IOUtils.copy(a,b)
  //  }

  def getConsoleMessage(): Unit = {

  }

  // do some test
  def main(args: Array[String]) {
    val configs = ConfigUtils.getConfig("/location.properties")
    val url = configs.getOrElse("county", "")
    //    val username = configs.get("REDIS_IP")
    //    val password = configs.get("MYSQL_DB_PASSWORD")
    //    val usrHome = System.getProperty("user.home")
    //    println(url)
    //    println(username)
    //    println(password)
  }
}