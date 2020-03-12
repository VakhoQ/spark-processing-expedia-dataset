package kafka

import java.text.SimpleDateFormat
import java.util.Date

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import schema.Booking

object KafkaUtils {

  val formater : SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  def csvToJson(line: String, date: Date) = {
    val cols: Array[String] = line.split(",").map(_.trim)
    val result :String = formater.format(date)
    val obj: Booking = new Booking(
      cols(0), cols(1), cols(2), cols(3), cols(4), cols(5), cols(6), cols(7), cols(8), cols(9), cols(10), cols(11),
      cols(12), cols(13), cols(14), cols(15), cols(16), cols(17), cols(18), cols(19), cols(20), cols(21), cols(22), cols(23), result
    );

    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val json: String = mapper.writeValueAsString(obj)
    json
  }



}
