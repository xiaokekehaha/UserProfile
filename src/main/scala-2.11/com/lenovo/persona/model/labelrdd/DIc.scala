package com.lenovo.persona.model.labelrdd

// scalastyle:off

import java.io.{File, PrintWriter}

import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._

object DIc {

  def main(args: Array[String]): Unit = {

    val city = scala.io.Source.fromFile("/Users/liziyao/Data/City").getLines().toList(0)

    val json = parse(city)

    val yixian = (json \ "一线城市").values.asInstanceOf[List[String]]
    val erxian_1 = (json \ "二线发达城市").values.asInstanceOf[List[String]]
    val erxian_2 = (json \ "二线中等发达城市").values.asInstanceOf[List[String]]
    val erxian_3 = (json \ "二线发展较弱城市").values.asInstanceOf[List[String]]
    val sanxian = (json \ "三线城市").values.asInstanceOf[List[String]]
    val sixian = (json \ "四线城市").values.asInstanceOf[List[String]]

    val dic_1 = yixian.map( x => x+"=1")
    val dic_2 = erxian_1.map( x => x+"=2")
    val dic_3 = erxian_2.map( x => x+"=3")
    val dic_4 = erxian_3.map( x => x+"=4")
    val dic_5 = sanxian.map( x => x+"=5")
    val dic_6 = sixian.map( x => x+"=6")

    val dic = dic_1 ++ dic_2 ++ dic_3 ++ dic_4 ++ dic_5 ++ dic_6

    val writer = new PrintWriter(new File("/Users/liziyao/Desktop/city.dic"))

    dic.foreach(writer.println)

    writer.close()


  }

}
