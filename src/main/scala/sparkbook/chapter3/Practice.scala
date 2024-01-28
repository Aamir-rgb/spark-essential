package sparkbook.chapter3

import org.apache.spark.sql.SparkSession

object Practice extends App {

  val spark = SparkSession.builder()
    .config("spark.master","local")
    .getOrCreate()

  val staticDataFrame = spark.read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load("src/main/resources/data/retail-data/by-day/*.csv")
  staticDataFrame.createOrReplaceTempView("retail_data")
  val staticSchema = staticDataFrame.schema

  import org.apache.spark.sql.functions.{window,column,desc,col}

  staticDataFrame.selectExpr("CustomerId","(UnitPrice*Quantity) as total_cost","InvoiceDate")
    .groupBy(col("CustomerId"),window(col("InvoiceDate"),"1 day"))
    .sum("total_cost")
    .show(5)

  spark.conf.set("spark.sql.shuffle.partitions","5")

  val streamingDataFrame = spark.readStream.schema(staticSchema)
    .option("maxFilesPerTrigger","1")
    .format("csv")
    .option("header","true")
    .load("src/main/resources/data/retail-data/by-day/*.csv")

  println(streamingDataFrame.isStreaming)
  // returns true

}
