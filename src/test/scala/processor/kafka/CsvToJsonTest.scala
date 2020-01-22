package processor.task1;


import java.io.File
import java.nio.file.Files
import java.util.concurrent.{ArrayBlockingQueue, Executor}
import java.util.logging.Level
import java.util.logging.Level.SEVERE

import org.apache.commons.io.{FileUtils, LineIterator}
import kafka.{EventHandler, KafkaUtils}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import schema.Booking

import scala.io.Source


@RunWith(classOf[JUnitRunner])
class CsvToJsonTest extends FunSuite {

  test("csv to json") {
//    val input: String = getClass.getResource("/train.csv").getPath

    val csvString = "2014-08-11 08:22:12,2,3,2,348,48862,,12,0,1,9,2014-08-29,2014-09-02,2,0,1,8250,1,1,1,1,2,3,1"
    val json = KafkaUtils.csvToJson(csvString)
    val expected = "{\"date_time\":\"2014-08-11 08:22:12\",\"site_name\":\"2\",\"posa_continent\":\"3\",\"user_location_country\":\"2\",\"user_location_region\":\"348\",\"user_location_city\":\"48862\",\"orig_destination_distance\":\"\",\"user_id\":\"12\",\"is_mobile\":\"0\",\"is_package\":\"1\",\"channel\":\"9\",\"srch_ci\":\"2014-08-29\",\"srch_co\":\"2014-09-02\",\"srch_adults_cnt\":\"2\",\"srch_children_cnt\":\"0\",\"srch_rm_cnt\":\"1\",\"srch_destination_id\":\"8250\",\"srch_destination_type_id\":\"1\",\"is_booking\":\"1\",\"cnt\":\"1\",\"hotel_continent\":\"1\",\"hotel_country\":\"2\",\"hotel_market\":\"3\",\"hotel_cluster\":\"1\"}";
    assert(json.equals(expected))
  }

}
