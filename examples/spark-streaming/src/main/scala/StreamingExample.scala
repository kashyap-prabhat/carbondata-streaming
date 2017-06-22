import org.apache.spark.sql.SparkSession

object StreamingExample extends App {

  val spark = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .master("local")
    .getOrCreate()

  import org.apache.spark.sql.types._

  val mySchema = new StructType().add("id", "integer").add("name", "string")
  val csvDF = spark.readStream
    .format("csv").option("sep", ",").schema(mySchema)
    .load("file:///home/prabhat/stream")

  csvDF.printSchema()

  val qry = csvDF.writeStream.format("carbondata").option("checkpointLocation", "file:///home/prabhat/ckpt")
    .option("path", "file:///home/prabhat/store/default/stream").start()

  qry.awaitTermination(10000)

}
