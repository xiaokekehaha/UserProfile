import com.lenovo.persona.utils.ConfigUtils

object TestNull {

  def main(args: Array[String]): Unit = {

    val configs = ConfigUtils.getConfig("/location.properties")
    val url = configs.getOrElse("county", "")
  }

}
