import org.apache.spark.sql.{SparkSession, DataFrame}
import io.delta.tables._
import org.apache.spark.sql.functions._
object Main {
  case class CustomerEvent(customer_id: String,event_type: String,timestamp: java.sql.Timestamp,value: Double)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DeltaLakeDemo")
      .config("spark.master", "local")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    import spark.implicits._
    val customerEvents = Seq(CustomerEvent("C1", "purchase", new java.sql.Timestamp(System.currentTimeMillis()), 100.0),
      CustomerEvent("C2", "view", new java.sql.Timestamp(System.currentTimeMillis()), 0.0)).toDF()

    customerEvents.write
      .format("delta")
      .partitionBy("event_type")
      .mode("append")
      .save("E:/events")
  }

}