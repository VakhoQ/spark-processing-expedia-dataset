package processor.task1

import helper.FileFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import processor.BaseProcessor

class TopHotelsDF  extends BaseProcessor{

  def process(spark: SparkSession, schema: StructType, input: String, output: String, inputFormat: FileFormat, isResultLoEnabled: Boolean) = {

    val dr = spark.read.format(inputFormat.toString)
      .option("header", "true")
      .schema(schema)
      .load(input)

    val df= dr.toDF();
    /**
     * Couples means == > SRCH_ADULTS_CNT==2
     * Group by 3 column
     * Desc sort
     */
    val result = df.filter("SRCH_ADULTS_CNT==2")
      .groupBy("hotel_continent", "hotel_country",  "hotel_market")
      .count().withColumnRenamed("count", "uniq_hotel_popularity_index")
      .sort(desc("uniq_hotel_popularity_index"))
      .limit(3)


    if(isResultLoEnabled){
      result.show()
    }


    result.write
      .format("csv")
      .option("header", "true")
      .csv(output)

  }


}
