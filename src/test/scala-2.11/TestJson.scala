import com.lenovo.persona.model.Up2Hive
import org.json4s._
import org.json4s.jackson.JsonMethods._

object TestJson {

  def main(args: Array[String]): Unit = {

    val str = "lenovoid:10043464013|lps_did:108544870237215|customid:120120323813153|lenovoid:10034014344|customid:120140410010073|lenovoid:10023124239|customid:120141115008692|lenovoid:10026831311|customid:120140226011176"
    val a = str.split("\\|").toList

    // {"lenovoid":["10043464013","10034014344","10023124239","10026831311"],"cs_customerid":[""],"microblogid":[""],"wechatid":[""],"mobile_md5":[""],"email":[""],"unique_cookie":[""],"imei_num":[""]}
    val json = Up2Hive.toJson(a)
    println(json)

    val lenovoid = parse(json) \ "email" \\ classOf[JString]

//    println(lenovoid(0) == "")





  }

}
