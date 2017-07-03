import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// scalastyle:off
object TestJson {

  def main(args: Array[String]): Unit = {

    val a = "{\"took\":21,\"timed_out\":false,\"_shards\":{\"total\":5,\"successful\":5,\"failed\":0},\"hits\":{\"total\":7998923,\"max_score\":1.0,\"hits\":[{\"_index\":\"profile4.\",\"_type\":\"userprofile\",\"_id\":\"s_478196842\",\"_score\":1.0,\"_source\":{\"superid\":[\"s_478196842\"],\"lenovoid\":[\"\"],\"service_users_id\":[\"\"],\"microblogid\":[\"\"],\"wechatid\":[\"\"],\"CP\":[\"\"],\"email\":[\"\"],\"unique_cookie\":[\"\"],\"imei_num\":[\"\"],\"lps_did\":[\"862578024082820\"]}},{\"_index\":\"profile4.\",\"_type\":\"userprofile\",\"_id\":\"s_950507929\",\"_score\":1.0,\"_source\":{\"superid\":[\"s_950507929\"],\"lenovoid\":[\"\"],\"service_users_id\":[\"\"],\"microblogid\":[\"\"],\"wechatid\":[\"\"],\"CP\":[\"\"],\"email\":[\"\"],\"unique_cookie\":[\"_sk201610162030320.69164000.2950\"],\"imei_num\":[\"\"],\"lps_did\":[\"\"]}},{\"_index\":\"profile4.\",\"_type\":\"userprofile\",\"_id\":\"s_950087110\",\"_score\":1.0,\"_source\":{\"superid\":[\"s_950087110\"],\"lenovoid\":[\"\"],\"service_users_id\":[\"\"],\"microblogid\":[\"\"],\"wechatid\":[\"\"],\"CP\":[\"\"],\"email\":[\"\"],\"unique_cookie\":[\"_sk201610150839400.66004500.7151\"],\"imei_num\":[\"\"],\"lps_did\":[\"\"]}},{\"_index\":\"profile4.\",\"_type\":\"userprofile\",\"_id\":\"s_478689175\",\"_score\":1.0,\"_source\":{\"superid\":[\"s_478689175\"],\"lenovoid\":[\"\"],\"service_users_id\":[\"\"],\"microblogid\":[\"\"],\"wechatid\":[\"\"],\"CP\":[\"\"],\"email\":[\"\"],\"unique_cookie\":[\"\"],\"imei_num\":[\"\"],\"lps_did\":[\"862576107433106\"]}},{\"_index\":\"profile4.\",\"_type\":\"userprofile\",\"_id\":\"s_948633909\",\"_score\":1.0,\"_source\":{\"superid\":[\"s_948633909\"],\"lenovoid\":[\"\"],\"service_users_id\":[\"\"],\"microblogid\":[\"\"],\"wechatid\":[\"\"],\"CP\":[\"\"],\"email\":[\"\"],\"unique_cookie\":[\"_sk201610132334160.54963500.2700\"],\"imei_num\":[\"\"],\"lps_did\":[\"\"]}},{\"_index\":\"profile4.\",\"_type\":\"userprofile\",\"_id\":\"s_1000021931\",\"_score\":1.0,\"_source\":{\"superid\":[\"s_1000021931\"],\"lenovoid\":[\"\"],\"service_users_id\":[\"\"],\"microblogid\":[\"\"],\"wechatid\":[\"\"],\"CP\":[\"\"],\"email\":[\"\"],\"unique_cookie\":[\"_sk201701080506460.47243500.3564\"],\"imei_num\":[\"\"],\"lps_did\":[\"\"]}},{\"_index\":\"profile4.\",\"_type\":\"userprofile\",\"_id\":\"s_478253422\",\"_score\":1.0,\"_source\":{\"superid\":[\"s_478253422\"],\"lenovoid\":[\"\"],\"service_users_id\":[\"\"],\"microblogid\":[\"\"],\"wechatid\":[\"\"],\"CP\":[\"\"],\"email\":[\"\"],\"unique_cookie\":[\"\"],\"imei_num\":[\"\"],\"lps_did\":[\"862578023930953\"]}},{\"_index\":\"profile4.\",\"_type\":\"userprofile\",\"_id\":\"s_952256095\",\"_score\":1.0,\"_source\":{\"superid\":[\"s_952256095\"],\"lenovoid\":[\"\"],\"service_users_id\":[\"\"],\"microblogid\":[\"\"],\"wechatid\":[\"\"],\"CP\":[\"\"],\"email\":[\"\"],\"unique_cookie\":[\"_sk201610200531250.52764900.9751\"],\"imei_num\":[\"\"],\"lps_did\":[\"\"]}},{\"_index\":\"profile4.\",\"_type\":\"userprofile\",\"_id\":\"s_951061028\",\"_score\":1.0,\"_source\":{\"superid\":[\"s_951061028\"],\"lenovoid\":[\"\"],\"service_users_id\":[\"\"],\"microblogid\":[\"\"],\"wechatid\":[\"\"],\"CP\":[\"\"],\"email\":[\"\"],\"unique_cookie\":[\"_sk201610151204150.72390800.1528\"],\"imei_num\":[\"\"],\"lps_did\":[\"\"]}},{\"_index\":\"profile4.\",\"_type\":\"userprofile\",\"_id\":\"s_950712744\",\"_score\":1.0,\"_source\":{\"superid\":[\"s_950712744\"],\"lenovoid\":[\"\"],\"service_users_id\":[\"\"],\"microblogid\":[\"\"],\"wechatid\":[\"\"],\"CP\":[\"\"],\"email\":[\"\"],\"unique_cookie\":[\"_sk201610171018170.53642700.4519\"],\"imei_num\":[\"\"],\"lps_did\":[\"\"]}}]}}"

    val json = parse(a)

    val super_id = (json \\ "superid" \\ classOf[JArray]).flatMap(x => x)

    super_id.foreach(println)
  }

}
