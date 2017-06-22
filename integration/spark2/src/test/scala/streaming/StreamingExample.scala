package streaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.CarbonSession

object StreamingExample extends App {

  val spark = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .getOrCreate()

  import spark.implicits._
  import org.apache.spark.sql.types._

  val mySchema = new StructType().add("id", "integer").add("name", "string")
  val csvDF = spark.readStream
    .format("csv").option("sep", ",").schema(mySchema)
    .load("/work/datadir")

  val qry = csvDF.writeStream.format("carbondata").option("checkpointLocation", "/work/ckpt")
    .option("path", "/work/existingTablePath").start()


}
