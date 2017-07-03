package com.lenovo.persona

import com.google.gson.JsonArray
import com.google.gson.JsonIOException
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.google.gson.JsonSyntaxException

// scalastyle:off
object JsonParse {

  def main(args: Array[String]): Unit = {

    val a = "{\"children\":[{\"children\":[{\"weight\":\"0.5903035\",\"labelName\":\"PC端\"}],\"labelName\":\"接触点偏好\"},{\"children\":[{\"weight\":\"0.5886996\",\"labelName\":\"12：00-14：00\"},{\"weight\":\"0.589104\",\"labelName\":\"8：00-12：00\"},{\"weight\":\"0.5892698\",\"labelName\":\"14：00-18：00\"},{\"weight\":\"0.5887031\",\"labelName\":\"0：00-8：00\"}],\"labelName\":\"接触时间段偏好\"}],\"labelName\":\"Think商城\"}"


    val pattern = """weight:*|labelName:*""".r

    val c = pattern.findAllMatchIn(a)

    c.foreach( x => println(x.matched))




  }

  def asd(json:JsonObject):(String,List[(String,String)]) = {


    ???
  }


}
