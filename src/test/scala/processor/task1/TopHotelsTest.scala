package processor.task1

;

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
class TopHotelsTest extends FunSuite {

  test("top hotels - dataset ") {

    val spark = SparkSession.builder().appName("Top 3 Booked").master("local[8]").getOrCreate()
    val schema = Encoders.product[Booking].schema
    val input: String = getClass.getResource("/train.csv").getPath

    val output: String = Files.createTempDirectory("spark").toAbsolutePath.toString + "/test/"

    // val input: String =  "/home/vq/hadoop/Expedia-Hotel-Recoomendations/train/train.csv"
    // val output: String = "/home/vq/spark/with_schema"

    FileUtils.deleteDirectory(new File(output).toPath.toFile)

    def top3 = new TopHotelsDF()

    top3.process(spark, schema, input, output, FileFormat.CSV, true)

    val dir = new File(output)
    val newFileRgex = ".*part-00000.*\\.csv"
    val csv = dir.listFiles.filter(_.toPath.toString.matches(newFileRgex))(0).toString

    val lines = Source.fromFile(csv).getLines.map(m => m.split(",")).toList

    assert(lines(1)(0) == "11")
    assert(lines(1)(1) == "22")
    assert(lines(1)(2) == "33")
    assert(lines(1)(3) == "3")

    if (lines(3)(0) == "911" && lines(2)(0) == "1") {
      assert(lines(3)(0) == "911")
      assert(lines(3)(1) == "223")
      assert(lines(3)(2) == "33")
      assert(lines(3)(3) == "2")
      assert(lines(2)(0) == "1")
      assert(lines(2)(1) == "2")
      assert(lines(2)(2) == "3")
      assert(lines(2)(3) == "2")
    } else {
      assert(lines(2)(0) == "911")
      assert(lines(2)(1) == "223")
      assert(lines(2)(2) == "33")
      assert(lines(2)(3) == "2")
      assert(lines(3)(0) == "1")
      assert(lines(3)(1) == "2")
      assert(lines(3)(2) == "3")
      assert(lines(3)(3) == "2")
    }

    FileUtils.deleteDirectory(new File(output).toPath.toFile)

    /*
     +---------------+-------------+------------+-----+
     |hotel_continent|hotel_country|hotel_market|count|
     +---------------+-------------+------------+-----+
     |             11|           22|          33|    3|
     |            911|          223|          33|    2|
     |              1|            2|           3|    2|
     +---------------+-------------+------------+-----+
    */

  }


  test("top hotels - dataframe ") {

    val spark = SparkSession.builder().appName("Top 3 Booked").master("local[8]").getOrCreate()


    val input: String = getClass.getResource("/train.csv").getPath

    val tempDirWithPrefix = Files.createTempDirectory("spark")
    val output: String = tempDirWithPrefix.toAbsolutePath.toString + "/test/"

    FileUtils.deleteDirectory(new File(output).toPath.toFile)

    def top3 = new TopHotelsDF()

    top3.process(spark, null, input, output, FileFormat.CSV, true)


    val dir = new File(output)
    val newFileRgex = ".*part-00000.*\\.csv"
    val csv = dir.listFiles.filter(_.toPath.toString.matches(newFileRgex))(0).toString

    val lines = Source.fromFile(csv).getLines.map(m => m.split(",")).toList

    assert(lines(1)(0) == "11")
    assert(lines(1)(1) == "22")
    assert(lines(1)(2) == "33")
    assert(lines(1)(3) == "3")


    if (lines(3)(0) == "911" && lines(2)(0) == "1") {
      assert(lines(3)(0) == "911")
      assert(lines(3)(1) == "223")
      assert(lines(3)(2) == "33")
      assert(lines(3)(3) == "2")
      assert(lines(2)(0) == "1")
      assert(lines(2)(1) == "2")
      assert(lines(2)(2) == "3")
      assert(lines(2)(3) == "2")
    } else {
      assert(lines(2)(0) == "911")
      assert(lines(2)(1) == "223")
      assert(lines(2)(2) == "33")
      assert(lines(2)(3) == "2")
      assert(lines(3)(0) == "1")
      assert(lines(3)(1) == "2")
      assert(lines(3)(2) == "3")
      assert(lines(3)(3) == "2")
    }

    FileUtils.deleteDirectory(new File(output).toPath.toFile)
    /*
     +---------------+-------------+------------+-----+
     |hotel_continent|hotel_country|hotel_market|count|
     +---------------+-------------+------------+-----+
     |             11|           22|          33|    3|
     |            911|          223|          33|    2|
     |              1|            2|           3|    2|
     +---------------+-------------+------------+-----+
    */

  }


}
