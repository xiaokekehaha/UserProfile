package com.lenovo.persona.json

// scalastyle:off
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.mutable.ArrayBuffer

object JsonPP {


  def main(args: Array[String]): Unit = {

    val file = "/Users/liziyao/Downloads/test.txt"

    val lines = scala.io.Source.fromFile(file).getLines().toList

    val jsonData = lines.map(x => {
      val json = compact(render(parse(x) \ "_data")).replaceAll("[\\[\\](){}]","").replaceAll("\"children\":","").split(",")
      json
    })

    jsonData.foreach(println)

  }

  def judge(list:List[String]):ArrayBuffer[(ArrayBuffer[String],ArrayBuffer[(String,String)])] = {
    val weight_label = new WL("","")
    for( i <- 0 until list.length) {
      val tmp = list(i)
      if(tmp.contains("weight") && check(weight_label) == 3){
        weight_label.a_=("")
      }



    }
    ???
  }

  def check(wl:WL):Int = {
    if(wl.weight.isEmpty)
      return 1
    else if (wl.label.isEmpty && !wl.weight.isEmpty)
      return 2
    else if (wl.weight.isEmpty && wl.label.isEmpty)
      return 3
    else
      return 4


  }

}
