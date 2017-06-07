import org.json4s._
import org.json4s.jackson.JsonMethods._

// scalastyle:off
object TestJson {

  def main(args: Array[String]): Unit = {

    val json = parse("""
         { "name": "joe",
           "children": ["q","w"
           ]
         }
       """)

    val a = (json \ "children").values.asInstanceOf[List[String]]


    println(a)



  }

}
