package com.lenovo.persona.model.labelrdd


// scalastyle:off
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SoftTime {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("NBM")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    //    val sqlContext = new SQLContext(sc)
    import hiveContext.sql

    val dataList = List("2017-06-07", "2017-06-06", "2017-05-31", "2017-05-30", "2017-05-29", "2017-05-28", "2017-05-27", "2017-05-26", "2017-05-25", "2017-05-24", "2017-05-23", "2017-05-22", "2017-05-21", "2017-05-20", "2017-05-19", "2017-05-18", "2017-05-17", "2017-05-16", "2017-05-15", "2017-05-14", "2017-05-13", "2017-05-12", "2017-05-11", "2017-05-10", "2017-05-09", "2017-05-08", "2017-05-07", "2017-05-06", "2017-05-05", "2017-05-04", "2017-05-03", "2017-05-02", "2017-05-01", "2017-04-30", "2017-04-29", "2017-04-28", "2017-04-27", "2017-04-26", "2017-04-25", "2017-04-24", "2017-04-23", "2017-04-22", "2017-04-21", "2017-04-20", "2017-04-19", "2017-04-18", "2017-04-17", "2017-04-16", "2017-04-15", "2017-04-14", "2017-04-13", "2017-04-12", "2017-04-11", "2017-04-10", "2017-04-09", "2017-04-08", "2017-04-07", "2017-04-06", "2017-04-05", "2017-04-04", "2017-04-03", "2017-04-02", "2017-04-01", "2017-03-31", "2017-03-30", "2017-03-29", "2017-03-28", "2017-03-27", "2017-03-26", "2017-03-25", "2017-03-24", "2017-03-23", "2017-03-22", "2017-03-21", "2017-03-20", "2017-03-19", "2017-03-18", "2017-03-17", "2017-03-16", "2017-03-15", "2017-03-14", "2017-03-13", "2017-03-12", "2017-03-11", "2017-03-10", "2017-03-09", "2017-03-08", "2017-03-07", "2017-03-06", "2017-03-05", "2017-03-04", "2017-03-03", "2017-03-02", "2017-03-01", "2017-02-28", "2017-02-27", "2017-02-26", "2017-02-25", "2017-02-23", "2017-02-22", "2017-02-21", "2017-02-20", "2017-02-19", "2017-02-18", "2017-02-17", "2017-02-16", "2017-02-15", "2017-02-14", "2017-02-13", "2017-02-12", "2017-02-11", "2017-02-10", "2017-02-09", "2017-02-08", "2017-02-07", "2017-02-06", "2017-02-05", "2017-02-04", "2017-02-03", "2017-02-02", "2017-02-01", "2017-01-31", "2017-01-30", "2017-01-29", "2017-01-28", "2017-01-27", "2017-01-26", "2017-01-25", "2017-01-24", "2017-01-23", "2017-01-22", "2017-01-21", "2017-01-20", "2017-01-19", "2017-01-18", "2017-01-17", "2017-01-16", "2017-01-15", "2017-01-14", "2017-01-13", "2017-01-12", "2017-01-11", "2017-01-10", "2017-01-09", "2017-01-08", "2017-01-07", "2017-01-06", "2017-01-05", "2017-01-04", "2017-01-03", "2017-01-02", "2017-01-01", "2016-12-31", "2016-12-30", "2016-12-29", "2016-12-28", "2016-12-27", "2016-12-26", "2016-12-25", "2016-12-24", "2016-12-23", "2016-12-22", "2016-12-21", "2016-12-20", "2016-12-19", "2016-12-18", "2016-12-17", "2016-12-16", "2016-12-15", "2016-12-14", "2016-12-13", "2016-12-12", "2016-12-11", "2016-12-10", "2016-12-09", "2016-12-08", "2016-12-07", "2016-12-06", "2016-12-05", "2016-12-04", "2016-12-03", "2016-12-02", "2016-12-01")

    dataList.foreach(x => {
      val date = x
      val begintime = System.currentTimeMillis()
      sql(s"with tem as ( SELECT a.lps_did,a.soft_name,b.software_category,  a.event_date closetime,b.event_date opentime,a.p_event_date  from  d_pc_ace.dwd_nb_software_software_close a  left join  d_pc_ace.dwd_nb_software_software_open_add_category  B   on a.p_event_date =b.p_event_date   and a.lps_did = b.lps_did  and a.soft_path = b.soft_path  where a.p_event_date ='$date' and b.p_event_date ='$date'  and a.event_date > b.event_date ),tem2 as ( SELECT lps_did,soft_name,software_category,closetime,  MAX(opentime) OVER(PARTITION BY lps_did,soft_name,closetime ORDER BY opentime) as opentime,  p_event_date  from tem ) insert OVERWRITE  TABLE d_dmp.service_l2_mid  PARTITION(ds) SELECT lps_did,soft_name,software_category, round((unix_timestamp(closetime,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(coalesce(opentime,closetime, opentime),'yyyy-MM-dd HH:mm:ss'))/3600,2) duration, p_event_date from tem2")
      val endtime = System.currentTimeMillis()
      val time = (endtime - begintime) / 1000 / 60
      sql(s"insert into d_lucp_dw.log select concat( $time,':',$date) from d_dmp.dual;")
    })
  }
}