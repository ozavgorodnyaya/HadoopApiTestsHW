import HWDataSet.{processTaxiData, readParquetDS}
import model.TaxiRide
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class DSApiUnitTest extends SharedSparkSession {

  test("test relation between count of passengers and trip distance") {
    val taxiDS: Dataset[TaxiRide] = readParquetDS("src/main/resources/data/yellow_taxi_jan_25_2018")

    val result: DataFrame = processTaxiData(taxiDS)

    checkAnswer(
      result,
      Row(0,2208,2.46,3.18,0.1,37.4) ::
      Row(1,241386,2.7,3.45,0.01,66.0) ::
      Row(2,44265,2.85,3.65,0.01,53.5) ::
      Row(3,11839,2.85,3.66,0.01,51.6) ::
      Row(4,5153,2.96,3.78,0.01,46.3) ::
      Row(5,15667,2.8,3.52,0.01,49.95) ::
      Row(6,9505,2.72,3.45,0.01,31.25)
        :: Nil
    )

  }
}