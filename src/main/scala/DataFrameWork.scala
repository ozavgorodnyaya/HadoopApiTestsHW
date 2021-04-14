import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.sql.{DataFrame, SparkSession}


object HWDataFrame {

  def readParquet(path: String)(implicit  spark: SparkSession): DataFrame = spark.read.load(path)
  def readCSV(path: String)(implicit  spark: SparkSession): DataFrame =
    spark.read
    .option("header","true")
    .option("inferSchema","true")
    .csv(path)

  def ProcessTaxiData(taxiDF: DataFrame, taxiZoneDF: DataFrame): DataFrame = {
    taxiDF
      .join(broadcast(taxiZoneDF), col(colName = "DOLocationID") === col(colName = "LocationID"), joinType = "left")
      .groupBy(col(colName = "Borough"))
      .count()
      .orderBy(col(colName = "count").desc)
  }

  def writeParquet (data: DataFrame, path: String) = {
    data.repartition(1)
      .write
      .format("parquet")
      .mode("append")
      .save(path)
  }

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession.builder()
      .appName("Joins")
      .config("spark.master", "local")
      .getOrCreate()

    val taxiZoneDF2 = readCSV("src/main/resources/data/taxi_zones.csv")
    val taxiDF2 = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")

    val result: DataFrame = ProcessTaxiData(taxiDF2,taxiZoneDF2)
    result.show()

    writeParquet(result,"src/main/resources/data/most_popular_boroughs.parquet")
  }
}