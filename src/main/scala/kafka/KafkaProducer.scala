package kafka

import java.util.{Properties, UUID}




object KafkaProducer
{
  def main(args: Array[String])
  {
    val input: String = "/home/vq/hadoop/Expedia-Hotel-Recoomendations/train/out.csv";
    val generator = new DataEventGenerator(1, "hotelReservation", input, "localhost:6667");
    generator.load()
  }
}
