import org.apache.spark.sql.{DataFrame, SparkSession}
import io.delta.tables._
import scalaj.http._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.json4s
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import java.util.Properties
object Main {
  Logger.getLogger("org").setLevel(Level.ERROR)
  case class ApiData(id:String,name: String,brewery_type: String)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DeltaLakeDemo")
      .config("spark.master", "local")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    val apiUrl = "https://api.openbrewerydb.org/breweries"
    val jsonResponse = callApi(apiUrl)
    val parsedJson = parseJson(jsonResponse)
    //print(parsedJson)
    implicit val formats = DefaultFormats
    val data = for{
      JObject(child)<- parsedJson
      JField("id",JString(id)) <- child
      JField("name",JString(name)) <- child
      JField("brewery_type",JString(brewery_type)) <- child
    } yield ApiData(id,name,brewery_type)
    import spark.implicits._
    val df = spark.createDataFrame(data)
    df.show()
    df.write
      .format("delta")
      .partitionBy("brewery_type")
      .mode("append")
      .save("E:/beverage")

//   def auditChanges(deltaTable: DeltaTable) = {
//     print("showing history")
//     deltaTable.history().show()
//   }

//    def validateData(df: DataFrame): DataFrame = {
//      df.withColumn("validation_status",
//          when(col("brewery_type").isNull || col("brewery_type") < 0, "Invalid")
//            .otherwise("Valid"))
//        .withColumn("validation_timestamp", current_timestamp())
//    }

    val deltaTable = DeltaTable.forPath(spark, "E:/beverage")
    //val validatedDF = validateData(deltaTable.toDF)

    val history = deltaTable.history()
    history.show()
}

  def callApi(url:String): String = {
    val response: HttpResponse[String] = Http(url).asString
    if(response.is2xx) {
      response.body
    } else {
      throw new RuntimeException(s"Failed to fetch data from API: ${response.code}")
    }
  }

  def parseJson(jsonStr: String): JValue = {
    parse(jsonStr)
  }

//  def auditChanges(deltaTable: DeltaTable) = {
//
//  }

}