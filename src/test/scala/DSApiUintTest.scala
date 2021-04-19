import HWDataSet.{processTaxiData, readParquetDS}
import model.TaxiRide
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class DSApiUnitTest extends SharedSparkSession {

  test("test relation between count of passengers and trip distance") {
    val taxiDS: Dataset[TaxiRide] = readParquetDS("src/main/resources/data/yellow_taxi_jan_25_2018")

    val result: DataFrame = processTaxiData(taxiDS)

//    checkAnswer(
//      result,
//      Row("passenger_count", 0, "cnt_trip", 2208, "avg_distance", 2.46, "stddev_pop_dist", 3.18,
//      "min_distance", 0.1, "max_distance", 37.4) ::
//      Row("passenger_count", 1, "cnt_trip", 241386, "avg_distance", 2.7, "stddev_pop_dist", 3.45,
//      "min_distance", 0.01, "max_distance", 66) ::
//      Row("passenger_count", 2, "cnt_trip", 44265, "avg_distance", 2.85, "stddev_pop_dist", 3.65,
//      "min_distance", 0.01, "max_distance", 53.5)
//        :: Nil
//    )

  }
}