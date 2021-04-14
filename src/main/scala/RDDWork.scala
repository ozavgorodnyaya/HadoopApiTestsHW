import java.nio.file._

import model.TaxiRide
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object HWRDD{

  def readParquetAsRDD(path: String)(implicit  spark: SparkSession): RDD[TaxiRide]  = {
    import model._
    import spark.implicits._

    val taxiFactsDF: DataFrame =
      spark.read
        .load("src/main/resources/data/yellow_taxi_jan_25_2018")

    val taxiFactsDS: Dataset[TaxiRide] =
      taxiFactsDF
        .withColumn("tpep_pickup_datetime",date_format(to_timestamp(col("tpep_pickup_datetime")),"HH"))
        .as[TaxiRide]

    val taxiFactsRDD: RDD[TaxiRide] =
      taxiFactsDS.rdd

    taxiFactsRDD
  }

  def processTaxiRDDData(data: RDD[TaxiRide]): Array[(String, Int)] = {
    val mappedTaxiFactsRDD :RDD[(String,Int)] = data
      .map(x => (x.tpep_pickup_datetime, 1))
    //.map(x => (x.tpep_pickup_datetime.split(" ")(1).split(":")(0), 1))

    val topOrderTime :Array[(String, Int)] =
      mappedTaxiFactsRDD
        .reduceByKey(_+_)
        .sortBy(_._2, false)
        .take(1)

    topOrderTime
  }

  def writeToTxtFile(arr: Array[(String, Int)], path: String) = {
    arr
      .foreach(d => Files.write(Paths.get(path),
        (d._1 + " " + d._2 + "\n").getBytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND))

  }

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession
      .builder()
      .appName("Introduction to RDDs")
      .config("spark.master", "local")
      .getOrCreate()

    val taxiFactsRDD: RDD[TaxiRide] = readParquetAsRDD("src/main/resources/data/yellow_taxi_jan_25_2018")

    val result: Array[(String, Int)] = processTaxiRDDData(taxiFactsRDD)

    result.foreach(x => println(x))

    writeToTxtFile(result,"src/main/resources/data/mostOrderTime.txt")
  }

}