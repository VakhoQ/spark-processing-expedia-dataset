package kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.Trigger
import schema.TestSchema

object KafkaConsumer
{
  def main(args: Array[String])
  {

//    val kafkaBrokers = "localhost:19092"

        val kafkaBrokers = "localhost:6667"


    val spark: SparkSession = SparkSession.builder().appName("Top 3 Booked").master("local[*]").getOrCreate()
//    val rawData = spark
//      .readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", kafkaBrokers)
//      .option("subscribe", "hotelReservation")
//      .option("startingOffsets", "latest")
//      .load()


//    val data = rawData
//      .withColumn("Key", col("key").cast(StringType))
//      .withColumn("Topic", $"topic".cast(StringType))
//      .withColumn("Offset", $"offset".cast(StringType))
//      .withColumn("Partition", $"partition".cast(StringType))
//      .withColumn("Timestamp", $"timestamp".cast(StringType))
//      .withColumn("Value", $"value".cast(StringType))
//      .withColumn("Key", $"key".cast(StringType))
//        .select("Key", "Value", "Partition", "Offset", "Timestamp")



    val rawData = spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", kafkaBrokers)
          .option("subscribe", "hotelReservation")
          .option("startingOffsets", "latest")
          .load()


    import spark.implicits._
//    val schema = new StructType()
//      .add($"id".string)
//      .add($"name".string)

    import org.apache.spark.sql.catalyst.ScalaReflection
    val schema = ScalaReflection.schemaFor[TestSchema].dataType.asInstanceOf[StructType]


//    val df = rawData
//      .select($"value" cast "string" as "json")
//      .select(from_json($"json", schema) as "data")
//      .select("data.*")
//      .withColumn("year", functions.date_format(col("name".trim), "YYYY"))
//      .withColumn("month", functions.date_format(col("name".trim), "MM"))
//      .withColumn("day", functions.date_format(col("name".trim), "dd"))
//      .coalesce(1)
//      .writeStream
//      .format("parquet")
//      .option("path", "hdfs://sandbox-hdp.hortonworks.com:8020/tmp/out19")
//      .option("checkpointLocation", "/tmp/out19")
//      .option("format", "complete")
//      .partitionBy("year")
//      .trigger(Trigger.ProcessingTime("60 seconds"))
//      .outputMode("append")
//      .start().awaitTermination()



        rawData
          .select($"value" cast "string" as "json")
          .select(from_json($"json", schema) as "data")
          .select("data.*")
          .withColumn("year", functions.date_format(col("name".trim), "YYYY"))
          .withColumn("month", functions.date_format(col("name".trim), "MM"))
          .withColumn("day", functions.date_format(col("name".trim), "dd"))
          .coalesce(1)
          .writeStream
          .outputMode("append")
          .format("console")
          .option("turnicate", false)
          .start().awaitTermination()




//
//        stream.print()
//        streamingContext.start()
//        streamingContext.awaitTermination()
  }
}
