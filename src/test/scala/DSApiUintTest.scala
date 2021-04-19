import HWDataSet.{processTaxiData, readParquetDS}
import model.TaxiRide
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class DFApiUnitTest extends SharedSparkSession {

  test("test calculation most popular boroughs for taxi orders") {
    val taxiDS: Dataset[TaxiRide] = readParquetDS("src/main/resources/data/yellow_taxi_jan_25_2018")

    val result: DataFrame = processTaxiData(taxiDS)

    checkAnswer(
      result,
      Row("trip_distance", 0.01, "cnt_trip", 103, "avg_distance", 0.01, "stddev_pop_dist", 0,
      "min_distance", 0.01, "max_distance", 0.01)
        :: Nil
    )

  }
}