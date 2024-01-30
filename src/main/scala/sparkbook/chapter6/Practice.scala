package sparkbook.chapter6

import org.apache.spark.sql.SparkSession

object Practice extends App {

  val spark = SparkSession.builder()
              .appName("DataFrame Operations")
              .config("spark.master","local")
              .getOrCreate()
  val df = spark.read.format("csv")
          .option("header","true")
          .option("inferSchema","true")
          .load("src/main/resources/data/retail-data/by-day/2010-12-01.csv")
  df.printSchema()
  df.createOrReplaceTempView("dfTable")

  import org.apache.spark.sql.functions.lit
  df.select(lit(5),lit("five"),lit(5.0))

  import org.apache.spark.sql.functions.col
  df.where(col("InvoiceNo").equalTo(536365))
    .select("InvoiceNo","Description")
    .show(5,false)

  df.where(col("InvoiceNo")===(536365))
    .select("InvoiceNo","Description")
    .show(5,false)

}
