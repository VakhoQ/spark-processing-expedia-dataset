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
import org.elasticsearch.spark.sql



/**
 *
 * @param kafkaBrokers  -broker location
 * @param checkpointLocation - checkpoint location
 * @param topicName - name of topic
 * @param format - ES for elastic search
 * @param esUrl - elastic node url
 * @param esPort - elastic node port
 * @param indexName - name of index
 */
class DataEventWriter(kafkaBrokers: String, checkpointLocation: String, topicName: String,  format: String, esUrl: String, esPort : String, indexName : String) {


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

            rawData
              .select($"value" cast "string" as "json")
              .select(from_json($"json", schema) as "data")
              .select("data.*")
              .writeStream
              .format("es")
              .option("es.port", esPort)
              .option("es.nodes", esUrl)
              .option("checkpointLocation", checkpointLocation)
              .trigger(Trigger.ProcessingTime("1 seconds"))
              .outputMode("Append")
              .start(indexName).awaitTermination()


  }

}


