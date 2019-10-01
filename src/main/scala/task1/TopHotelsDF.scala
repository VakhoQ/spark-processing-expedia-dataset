package task1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TopHotelsDF {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("GCS Extractor Application").master("local[4]")
      .getOrCreate()

    val parsedRdd = spark.read.format("csv").option("header", "true").load("/home/vq/IdeaProjects/scala-first/src/main/resources/train.csv")

     val df= parsedRdd.toDF();

    /**
     * Couples means == > SRCH_ADULTS_CNT==2
     */
     df.filter("SRCH_ADULTS_CNT==2")
     .groupBy("hotel_continent", "hotel_country",  "hotel_market")
     .count().withColumnRenamed("count", "uniq_hotel_popularity_index")
     .sort(desc("uniq_hotel_popularity_index"))
     .show(3)


  }



}
