package processor.task3

/*
 * This Scala Testsuite was generated by the Gradle 'init' task.
 */
import java.io.File
import java.nio.file.Files
import org.apache.commons.io.FileUtils
import helper.FileFormat
import org.apache.spark.sql.{Encoders, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import schema.Booking

import scala.io.Source


@RunWith(classOf[JUnitRunner])
class Top3NotBookedHotelTest extends FunSuite {

  test("top 3 not booked hotel test") {

    val spark = SparkSession
      .builder()
      .appName("Top 3 Booked").master("local[4]")
      .getOrCreate()

    val schema = Encoders.product[Booking].schema

    val input: String =  getClass.getResource("/train.csv").getPath

    val tempDirWithPrefix = Files.createTempDirectory("spark")
    val output: String = tempDirWithPrefix.toAbsolutePath.toString + "/test/"
    FileUtils.deleteDirectory(new File(output).toPath.toFile)

    def top3 = new Top3NotBooked()
    top3.process(spark, schema, input,output, FileFormat.CSV , true)


    val dir = new File(output)
    val newFileRgex =  ".*part-00000.*\\.csv"
    val csv  = dir.listFiles.filter(_.toPath.toString.matches(newFileRgex))(0).toString

    println("FINISH")

    val lines  = Source.fromFile(csv).getLines.map(m=>m.split(",")).toList

    assert( lines(1)(0) == "33" )
    assert( lines(1)(1) == "3" )
    assert( lines(2)(0) == "000" )
    assert( lines(2)(1) == "1" )

    FileUtils.deleteDirectory(new File(output).toPath.toFile)

    /*
    +------------+-----+
    |hotel_market|count|
    +------------+-----+
    |          33|    3|
    |         000|    1|
    +------------+-----+
     */

  }
}
