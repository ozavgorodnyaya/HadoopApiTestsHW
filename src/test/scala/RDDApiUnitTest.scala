import HWRDD.{readParquetAsRDD,processTaxiRDDData}
import model.TaxiRide
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class RDDApiUnitTestTest extends AnyFlatSpec {

  implicit val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Test for RDD")
    .getOrCreate()

  it should "Successfully upload and process RDD data" in {
    val taxiFactsRDD: RDD[TaxiRide] = readParquetAsRDD("src/main/resources/data/yellow_taxi_jan_25_2018")

    val actualResult = processTaxiRDDData(taxiFactsRDD)

    assert(actualResult === Array(("19",22121)))
//    assert(actualResult(0)._1 == "19")
//    assert(actualResult(0)._2 == 22121)
  }

}