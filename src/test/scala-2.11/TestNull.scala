import java.text.SimpleDateFormat

import com.lenovo.persona.utils.ConfigUtils

// scalastyle:off

object TestNull {

  def main(args: Array[String]): Unit = {

    //    val configs = ConfigUtils.getConfig("/location.properties")
    //    val url = configs.getOrElse("county", "")


    //    val a = "20160909"
    //    val b = "0000000055d5"
    //
    //
    //    println(b.hashCode)
    //
    ////    println(dateTrans(a))
    //  }
    //
    //
    //  def dateTrans(src: String): String = {
    //    val spdf = new SimpleDateFormat("yyyyMMdd")
    //    val date = spdf.parse(src)
    //    val spdf2 = new SimpleDateFormat("yyyy-MM-dd")
    //    spdf2.format(date)
    //  }

    val cityMap = ConfigUtils.getConfig("/city.properties")
    val a = "太原市"
    val aa = cityMap.getOrElse(a,0)
    println(aa.toString.toInt)
//    cityMap.foreach(println)

  }
}