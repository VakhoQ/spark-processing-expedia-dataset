package processor.task2

import helper.FileFormat
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession}
import processor.BaseProcessor
import schema.Booking

class BookedFromSameCountry extends BaseProcessor {


  def process(spark: SparkSession, schema: StructType, input: String, output: String, inputFormat: FileFormat, isResultLoEnabled: Boolean) = {
    import spark.implicits._

    val dr = spark.read.format(inputFormat.toString)
      .option("header", "true")
      .schema(schema)
      .load(input)

    val ds: Dataset[Booking] = dr.as[Booking]

    val result =  ds.filter(p =>p.is_booking != 0)
      .filter(p =>p.user_location_country == p.hotel_country)
      .groupBy($"hotel_country")
      .count()
      .sort(desc("count"))
      .limit(1)


    if(isResultLoEnabled){
      result.show()
    }


    result.write
      .format("csv")
      .option("header", "true")
      .csv(output)

  }




}
