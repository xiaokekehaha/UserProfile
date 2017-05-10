import com.lenovo.persona.model.Up2Hive


object TestJson {

  def main(args: Array[String]): Unit = {

    val str = "lenovoid:10043464013|lps_did:108544870237215|customid:120120323813153|lenovoid:10034014344|customid:120140410010073|lenovoid:10023124239|customid:120141115008692|lenovoid:10026831311|customid:120140226011176"
    val a = str.split("\\|").toList

    println(Up2Hive.toJson(a))


  }

}
