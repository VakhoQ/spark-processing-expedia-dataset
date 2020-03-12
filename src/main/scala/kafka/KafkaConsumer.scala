package kafka

object KafkaConsumer
{
  def main(args: Array[String])
  {
    val writer = new DataEventWriter(
      "localhost:6667",
      "/tmp/bookingEvent4",
      "hotelReservation",
      "es",
      "http://hdp-proxy",
      "9200",
      "expediabooking1/_doc"

    )

    writer.load()
  }
}
