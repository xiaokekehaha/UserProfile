package com.lenovo.persona.json

// scalastyle:off
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.mutable.ArrayBuffer

object JsonPP {


  def main(args: Array[String]): Unit = {

    val file = "/Users/liziyao/Downloads/user.list"
    val dic = "/Users/liziyao/Data/level.txt"

    val code = scala.io.Source.fromFile(dic)
      .getLines()
      .toList
      .map(_.split(","))
      .map(x => {
        val tagcode = x(0)
        val label = x(2).split("/").last + "_" + x(1)
        (label,tagcode)
      }).toMap

    val lines = scala.io.Source.fromFile(file).getLines().toList.map( x => {
      val aa = x.split("  ,",-1)
      val id = aa(0)
      val jsons = aa(1)
      (id,jsons)
    })
    val jsonData = lines.map(x => {
      val id = x._1
      val title = try{compact(render(parse(x._2) \ "ids")) + "," + compact(render(parse(x._2) \ "survey"))} catch {
        case ex:Exception => ""
      }
      val json = try{compact(render(parse(x._2) \ "_data")).replaceAll("[\\[\\](){}]", "").replaceAll("\"children\":", "").split(",").toList} catch {
        case ex:Exception => List.empty[String]
      }
//      println(json)
      val result = new ArrayBuffer[(ArrayBuffer[String], ArrayBuffer[WL])]
      val labels = new ArrayBuffer[String]()
      val values = new ArrayBuffer[WL]()
      val tmp = new WL
      tmp.weight_=("")
      tmp.label_=("")
      values += tmp
      val asd = judge(json, 0, result, labels, values)
      (id,title, asd)
    })

    val ree = jsonData.map(x => {
      val id = x._1
      val title = x._2
      val data = x._3.flatMap(y => {
        val labels = y._1.reverse.map(replaces).last
        val values = y._2.map(z => {
//          (labels + "_" + replaces(z.label),replaces(z.weight))
          val key = labels + "_" + replaces(z.label)
          val weight = replaces(z.weight)
          val tagvalue = code.getOrElse(key,key)
          val tagid = if(tagvalue == key) "" else tagvalue.substring(0,tagvalue.size-5)
          tagid+":"+tagvalue+":"+weight
        })
        values.toList
      })
      (id,title, data)
    })

    ree.foreach(println)

    //    jsonData.foreach(println)

  }



  def judge(list: List[String], index: Int, result: ArrayBuffer[(ArrayBuffer[String], ArrayBuffer[WL])], labels: ArrayBuffer[String], values: ArrayBuffer[WL]): ArrayBuffer[(ArrayBuffer[String], ArrayBuffer[WL])] = {
    if (index >= list.size) {
      result
    } else if (list(index).contains("weight") && check(values.last) == 1) {
      values.last.weight_=(list(index))
      judge(list, index + 1, result, labels, values)
    } else if (list(index).contains("labelName") && check(values.last) == 2) {
      values.last.label_=(list(index))
      judge(list, index + 1, result, labels, values)
    } else if (list(index).contains("weight") && check(values.last) == 3 && !checkLabel(labels)) {
      // labels为空
      val wl = new WL
      wl.weight_=(list(index))
      values += wl
      judge(list, index + 1, result, labels, values)
    } else if (list(index).contains("weight") && check(values.last) == 3 && checkLabel(labels)) {
      // labels不为空
      val wl = new WL
      wl.weight_=(list(index))
      val line = (labels, values)
//            println(line)
      result += line
      val labels_2 = new ArrayBuffer[String]()
      val values_2 = new ArrayBuffer[WL]()
      val tmp = new WL
      tmp.weight_=("")
      tmp.label_=("")
      values_2 += tmp
      judge(list, index + 1, result, labels_2, values_2)
    } else if (list(index).contains("labelName") && check(values.last) == 3) {
      // 不区分labels是空还是有值
      labels += list(index)
      judge(list, index + 1, result, labels, values)
    } else {
      judge(list, index + 1, result, labels, values)
    }
  }

  def check(wl: WL): Int = {
    if (wl.weight.isEmpty)        // weight 为空
      1
    else if (wl.label.isEmpty && !wl.weight.isEmpty)        // weight不为空，label为空
      2
    else if (!wl.weight.isEmpty && !wl.label.isEmpty)       // weight label 都不为空
      3
    else
      4
  }

  def checkLabel(labels: ArrayBuffer[String]): Boolean = {
    if (labels.isEmpty)
      false
    else
      true
  }

  def replaces(str: String): String = {
    str.replaceAll("[\":]", "").replaceAll("labelName", "").replaceAll("weight", "")
  }

}
