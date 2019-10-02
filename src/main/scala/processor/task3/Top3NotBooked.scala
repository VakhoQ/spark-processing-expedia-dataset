package processor.task3

import helper.FileFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession}
import schema.Booking
import org.apache.spark.sql.functions.desc
import processor.BaseProcessor

class Top3NotBooked extends BaseProcessor {


   def process(spark: SparkSession, schema: StructType, input: String, output: String, inputFormat: FileFormat, isResultLoEnabled: Boolean) = {
    import spark.implicits._

    val dr = spark.read.format(inputFormat.toString)
      .option("header", "true")
      .schema(schema)
      .load(input)

    val ds: Dataset[Booking] = dr.as[Booking]

     /**
      * Filter is_booking= 0 and srch_children_cnt > 0
      * Group by hotel market
      * count top 3
      */
    val result =
       ds.filter(p => p.is_booking == 0)
      .filter(p2 => p2.srch_children_cnt!=0)
      .groupBy($"hotel_market")
      .count()
      .sort(desc("count"))
      .limit(3);

    if(isResultLoEnabled){
      result.show()
    }

    result.write
      .format("csv")
      .option("header", "true")
      .csv(output)


  }
}
