package processor

import helper.FileFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

/**
 * Main Trait Processor
 */
trait BaseProcessor {
  /**
   * data processor method that is responsible to return result in the output parameter
   * @param spark Spark Session Object
   * @param schema Schema of the object
   * @param input Input path of the file
   * @param output Output path of the result
   * @param inputFormat enum of supported formats
   * @param isResultLoEnabled if true result data will be logged
   */
  def process(spark: SparkSession, schema: StructType, input: String, output: String, inputFormat: FileFormat, isResultLoEnabled: Boolean)
}
