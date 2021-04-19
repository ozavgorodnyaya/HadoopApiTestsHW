import HWDataFrame.{ProcessTaxiData, readCSV, readParquet}
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{DataFrame, Row}

class DFApiUnitTest extends SharedSparkSession {

  test("test calculation most popular boroughs for taxi orders"){
    val taxiDF2 = readParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
    val taxiZoneDF2 = readCSV("src/main/resources/data/taxi_zones.csv")

    val result: DataFrame = ProcessTaxiData(taxiDF2,taxiZoneDF2)

    checkAnswer(
      result,
      Row("Manhattan",296527)  ::
        Row("Queens",13819) ::
        Row("Brooklyn",12672) ::
        Row("Unknown",6714) ::
        Row("Bronx",1589) ::
        Row("EWR",508) ::
        Row("Staten Island",64) :: Nil
    )

  }

}