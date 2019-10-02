package processor.task2;

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
class SameCountryBookTest extends FunSuite {


    test("not booked hotels") {

        val spark = SparkSession
                .builder()
                .appName("Top 3 Booked").master("local[4]")
                .getOrCreate()

        val schema = Encoders.product[Booking].schema

        val input: String =  getClass.getResource("/train.csv").getPath


        val tempDirWithPrefix = Files.createTempDirectory("spark")
        val output: String = tempDirWithPrefix.toAbsolutePath.toString + "/test/"
        FileUtils.deleteDirectory(new File(output).toPath.toFile)

        def top3 = new BookedFromSameCountry()
        top3.process(spark, schema, input,output, FileFormat.CSV , true)


        val dir = new File(output)
        val newFileRgex =  ".*part-00000.*\\.csv"
        val csv  = dir.listFiles.filter(_.toPath.toString.matches(newFileRgex))(0).toString

        println("FINISH")

        val lines  = Source.fromFile(csv).getLines.map(m=>m.split(",")).toList


        assert( lines(1)(0) == "22" )
        assert( lines(1)(1) == "2" )

        FileUtils.deleteDirectory(new File(output).toPath.toFile)

      /*

        +-------------+-----+
        |hotel_country|count|
        +-------------+-----+
        |           22|    2|
        +-------------+-----+

         */

    }


}
