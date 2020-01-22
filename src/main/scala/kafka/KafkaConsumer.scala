package kafka

object KafkaConsumer
{
  def main(args: Array[String])
  {
    val writer = new DataEventWriter(
      false,
      "localhost:6667",
      "hdfs://sandbox-hdp.hortonworks.com:8020/tmp/test30s_4",
      "/tmp/test30s_4",
      "hotelReservation",
      "parquet"
    )

    writer.load()
  }
}
