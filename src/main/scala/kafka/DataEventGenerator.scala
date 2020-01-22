package kafka

import java.io.File
import java.util.{Properties, UUID}
import java.util.concurrent.{BlockingQueue, CountDownLatch, ExecutorService, Executors, LinkedBlockingDeque, Semaphore, TimeUnit}
import java.util.logging.Level.SEVERE
import java.util.logging.{Level, Logger}

import org.apache.commons.io.{FileUtils, LineIterator}


/**
 *
 * @param poolSize - parallelization factor
 * @param topicName - name of topic
 * @param inputFilePath - input
 * @param kafkaBrokers - broker location
 */
class DataEventGenerator(poolSize: Integer, topicName: String, inputFilePath: String, kafkaBrokers: String) {

  val log: Logger = Logger.getLogger(this.getClass.getName);
  val pool: ExecutorService = Executors.newFixedThreadPool(poolSize)
  val semaphore: Semaphore = new Semaphore(0)

  def load() {
    val prop = new Properties()
    prop.put("bootstrap.servers", kafkaBrokers)
    prop.put("acks", "all")
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer: org.apache.kafka.clients.producer.KafkaProducer[String, String] = new org.apache.kafka.clients.producer.KafkaProducer[String, String](prop)

    val theFile: File = new File(inputFilePath)
    var it: LineIterator = null

    var numberOfTasks: Integer = 0
    try {
      it = FileUtils.lineIterator(theFile, "UTF-8")
      val header = it.nextLine
      log.info("header: " + header)
      while (it.hasNext) {
        numberOfTasks += 1
        val line = it.nextLine
        log.info("processing record: " + line)
        pool.execute(new EventHandler(line, producer, topicName, semaphore))

      }

      log.info("all the tasks has been submitted")
      pool.shutdown()
      log.info("pool has been shut down not to accept new tasks")

      try {
        semaphore.acquire(numberOfTasks)
      } catch {
        case x: Exception =>
          log.log(Level.SEVERE, "thread was terminated")
      }
      log.info("all tasks has been executed")
    }
    catch {
      case x: Exception =>
        log.log(SEVERE, "error: ", x)
    } finally {
      producer.flush()
    }

  }

}




