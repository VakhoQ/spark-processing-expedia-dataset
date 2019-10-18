package processor.task1

import helper.FileFormat
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import processor.BaseProcessor
import schema.Booking

class TopHotelsDF  extends BaseProcessor{



  def process(spark: SparkSession, schema: StructType, input: String, output: String, inputFormat: FileFormat, isResultLoEnabled: Boolean) = {

    import spark.implicits._

    val dr = crateDataFrameFromFile(spark, schema, input, inputFormat)

    var result : Dataset[Row] = null;

    /**
     * Couples means == > SRCH_ADULTS_CNT==2
     * Group by 3 column
     * Desc sort
     */

    if(schema != null){
      val ds: Dataset[Booking] = dr.as[Booking]
       result = ds.filter(p => p.srch_adults_cnt.equalsIgnoreCase("2"))
        .groupBy($"hotel_continent", $"hotel_country",  $"hotel_market")
        .count()
         .sort($"count".desc)
        .limit(3)
    }else{
         result = dr.filter("SRCH_ADULTS_CNT==2")
        .groupBy("hotel_continent", "hotel_country",  "hotel_market")
        .count()
        .sort(desc("count"))
        .limit(3)
    }

    if(isResultLoEnabled){
      result.show()
    }

    result.write
      .format("csv")
      .option("header", "true")
      .csv(output)

  }


  private def crateDataFrameFromFile(spark: SparkSession, schema: StructType, input: String, inputFormat: FileFormat): DataFrame = {
    if (schema == null) {
      spark.read.format(inputFormat.toString)
        .option("header", "true")
        .load(input)
    } else {
      spark.read.format(inputFormat.toString)
        .option("header", "true")
        .schema(schema)
        .load(input)
    }
  }
}
