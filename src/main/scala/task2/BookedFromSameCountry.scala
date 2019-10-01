package task2

import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import schema.Booking

object BookedFromSameCountry {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("GCS Extractor Application").master("local[4]")
      .getOrCreate()
    import spark.implicits._

    val schema = Encoders.product[Booking].schema

    val parsedRdd = spark.read.format("csv").option("header", "true").schema(schema)
      .load("/home/vq/IdeaProjects/scala-first/src/main/resources/train.csv")

    val ds: Dataset[Booking] = parsedRdd.as[Booking]

    ds
      .filter(p =>p.is_booking != 0)
      .filter(p =>p.user_location_country == p.hotel_country)
      .groupBy($"hotel_country")
      .count()
      .sort(desc("count"))
      .show(1)


  }



}
