package com.lenovo.persona

import java.io.{File, PrintWriter}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}

// scalastyle:off
object LineTrace {

  def main(args: Array[String]): Unit = {


//    all_buy.txt             app_first_download.txt  create_lenovo.txt       device_time.txt         log_web_record.txt
//    app_download_count.txt  browing_forum_count.txt device_input.txt        log_web_count.txt       posting.txt

    val file = "xad"
    val path = s"/Users/liziyao/Downloads/bojie_result/$file"
    val result = s"/Users/liziyao/Downloads/bojie_result_new/$file"
    val writer = new PrintWriter(new File(result))

    val lines = scala.io.Source.fromFile(path).getLines().toList
    val results = lines.zipWithIndex.map( x => {
      var content = x._1
      val id = x._2 +2 + 9000000 + 9000000 + 9000000
      if(content.contains("index")){
        val indexJson = (
          ("index" -> ("_id" -> id/2))
          )
        content = compact(render(indexJson))
      }
      content
    })

    results.foreach(writer.println)

    writer.close()
  }

}
