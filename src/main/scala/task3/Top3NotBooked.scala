package task3

import helper.FileFormat
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession}
import schema.Booking
import org.apache.spark.sql.functions.desc

class Top3NotBooked {


  def main(args: Array[String]) {



  }

   def findTop3Hotel(spark: SparkSession, schema: StructType, input: String, output: String, inputFormat: FileFormat, isResultLoEnabled: Boolean) = {
    import spark.implicits._

    val dr = spark.read.format(inputFormat.toString)
      .option("header", "true")
      .schema(schema)
      .load(input)

    val ds: Dataset[Booking] = dr.as[Booking]

 //&&  p.is_booking == 0, p.srch_children_cnt != 0
    val result =
       ds.filter(p => p.is_booking == 0)
      .filter(p2 => p2.srch_children_cnt!=0)
      .groupBy($"hotel_market")
      .count()
      .sort(desc("count"))
      .limit(10);

    if(isResultLoEnabled){
      result.show()
    }

    result.write
      .format("csv")
      .option("header", "true")
      .csv(output)


  }
}
