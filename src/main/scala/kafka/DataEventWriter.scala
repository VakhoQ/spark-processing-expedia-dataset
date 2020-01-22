package kafka

import java.io.File
import java.util.concurrent.{ExecutorService, Executors, Semaphore}
import java.util.logging.Level.SEVERE
import java.util.logging.{Level, Logger}
import java.util.{Properties, UUID}

import org.apache.commons.io.{FileUtils, LineIterator}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import schema.Booking

/**
 *
 * @param debugMode - if debug mode is true, we only print out the results in the console
 * @param kafkaBrokers - broker location
 * @param hdfsPath - hdfs output path location
 * @param checkpointLocation - checkpoint location
 * @param topicName - name of topic
 * @param format - format can be CSV , parquet etc.
 */
class DataEventWriter(debugMode: Boolean, kafkaBrokers: String, hdfsPath: String, checkpointLocation: String, topicName: String,  format: String) {


  def load() {

    val spark: SparkSession = SparkSession.builder().appName("Event processor").master("local[*]").getOrCreate()
    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

    val rawData = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", topicName)
      .option("startingOffsets", "latest")
      .load()

    import spark.implicits._
    val schema = ScalaReflection.schemaFor[Booking].dataType.asInstanceOf[StructType]

      if(debugMode) {
        rawData
          .select($"value" cast "string" as "json")
          .select(from_json($"json", schema) as "data")
          .select("data.*")
          .withColumn("year", functions.date_format(col("date_time".trim), "YYYY"))
          .withColumn("month", functions.date_format(col("date_time".trim), "MM"))
          .withColumn("day", functions.date_format(col("date_time".trim), "dd"))
          .coalesce(1)
          .writeStream
          .outputMode("append")
          .format("console")
          .option("turnicate", false)
          .start().awaitTermination()
      }else{
            rawData
              .select($"value" cast "string" as "json")
              .select(from_json($"json", schema) as "data")
              .select("data.*")
              .withColumn("year", functions.date_format(col("date_time".trim), "YYYY"))
              .withColumn("month", functions.date_format(col("date_time".trim), "MM"))
              .withColumn("day", functions.date_format(col("date_time".trim), "dd"))
              .coalesce(1)
              .writeStream
              .format(format)
              .option("path", hdfsPath)
              .option("checkpointLocation", checkpointLocation)
              .option("format", "complete")
              .partitionBy("year", "month")
              .trigger(Trigger.ProcessingTime("30 seconds"))
              .outputMode("append")
              .start().awaitTermination()
      }

  }

}


