import java.util.Properties

import model.TaxiRide
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object HWDataSet {

  def readParquetDS(path: String)(implicit  spark: SparkSession): Dataset[TaxiRide] = {
    val taxiFactsDF: DataFrame =
      spark.read
        .load(path)

    import model._
    import spark.implicits._

    val taxiFactsDS: Dataset[TaxiRide] =
      taxiFactsDF
        .as[TaxiRide]
    taxiFactsDS
  }

  def processTaxiData(data: Dataset[TaxiRide]): DataFrame = {
    data
        .filter(x => x.trip_distance != 0)
        .groupBy(col("trip_distance"))
        .agg(count(data.col("trip_distance")),
          avg(data.col("trip_distance")),
          stddev_pop(data.col("trip_distance")),
          min(data.col("trip_distance")),
          max(data.col("trip_distance")))
        .orderBy(col("trip_distance"))
  }

    def writeToPostgre(driver: String, url: String,
                       user: String, pass: String,
                       table: String, df: DataFrame) = {
      val properties = new Properties()
      properties.setProperty("user", user)
      properties.setProperty("password", pass)
      properties.put("driver", driver)

      df.write.mode(Overwrite).jdbc(url, table, properties)
    }

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession
      .builder()
      .appName("Introduction to DataSet")
      .config("spark.master", "local")
      .getOrCreate()

    val taxiDS: Dataset[TaxiRide] = readParquetDS("src/main/resources/data/yellow_taxi_jan_25_2018")

    val result: DataFrame = processTaxiData(taxiDS)
    result.show()

    val driver = "org.postgresql.Driver"
    val url = "jdbc:postgresql://localhost:5432/docker"
    val user = "docker"
    val password = "docker"
    val tableName = "trip_distance_info"

    writeToPostgre(driver,url,
                   user,password,
                   tableName,result)
  }

}