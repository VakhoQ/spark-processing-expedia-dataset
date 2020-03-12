package kafka

import java.util.{Date, UUID}
import java.util.concurrent.Semaphore
import java.util.logging.Logger

class EventHandler(json: String, producer: org.apache.kafka.clients.producer.KafkaProducer[String, String], topicName: String, semaphore: Semaphore) extends Runnable {
  val log: Logger = Logger.getLogger(this.getClass.getName)
  def run() {
    val msg: String = KafkaUtils.csvToJson(json, new Date())
    log.info(Thread.currentThread().getName() + " sending json: " + msg)
    producer.send(new org.apache.kafka.clients.producer. ProducerRecord[String,String](topicName, UUID.randomUUID().toString, msg))
    semaphore.release()
  }
}
