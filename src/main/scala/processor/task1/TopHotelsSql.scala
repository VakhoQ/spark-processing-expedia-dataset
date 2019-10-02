package processor.task1

import org.apache.spark.sql.SparkSession

object TopHotelsSql {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("GCS Extractor Application").master("local[4]")
      .getOrCreate()

    val df = spark.read.format("csv").option("header", "true")
      .load("/home/vq/IdeaProjects/scala-first/src/main/resources/train.csv")

    df.createOrReplaceTempView("TRAIN_FROM_AVRO_SCHEMA")

    val sql : String =
      "SELECT HOTELS.HOTEL, \n" +
        "COUNT(HOTELS.HOTEL) AS COUNT \n" +
        "FROM (\n" +
        " SELECT  \n" +
        " CONCAT('  ',hotel_continent, '  ',hotel_country,' ' ,hotel_market) AS HOTEL \n" +
        " FROM TRAIN_FROM_AVRO_SCHEMA \n" +
        " where SRCH_ADULTS_CNT=2\n" +
        ") HOTELS \n" +
        "GROUP BY HOTELS.HOTEL \n" +
        "ORDER BY COUNT DESC LIMIT 3";


    val sqlDF = spark.sql(sql)
    sqlDF.show()



  }



}
