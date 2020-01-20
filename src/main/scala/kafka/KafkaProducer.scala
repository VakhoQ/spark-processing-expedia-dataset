package kafka

import java.util.{Properties, UUID}




object KafkaProducer
{
  def main(args: Array[String])
  {

    val topicName = "hotelReservation"
//    val kafkaBrokers = "localhost:19092"
   val kafkaBrokers = "localhost:6667"

    val prop = new Properties()
    prop.put("bootstrap.servers", kafkaBrokers)
    prop.put("acks", "all")
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new org.apache.kafka.clients.producer.KafkaProducer [String, String](prop)


    var  i : Integer = 0;
    while (true){
      i = i+1;

      if(i%2 ==0) {
        sendEvent("{  \"id\":30 , \"name\":\"2014-08-11" +"\" }")
      }else {
        sendEvent("{  \"id\":30 , \"name\":\"2018-07-10" +"\" }")
      };
       Thread.sleep(1)
    }


    sendEvent("hello vakhtang" + 9)

    def sendEvent(message: String ) = {
      val key = UUID.randomUUID().toString();
      producer.send(new org.apache.kafka.clients.producer. ProducerRecord[String,String](topicName, key, message))
      System.out.println("sent message. Key: " + key + " value " + message + " \n ")
    }

  }
}
