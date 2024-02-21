package part2dataframes

import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{SparkSession}
import part2dataframes.Aggregations
import org.apache.spark.sql.functions._


object Aggregations extends App {
  //Create Spark Session
  val spark = SparkSession.builder()
             .appName("Aggregation and Groupings")
             .config("spark.master","local")
             .getOrCreate()
  val moviesDF = spark.read.option("inferSchema","true").json("src/main/resources/data/movies.json")
  println(moviesDF.count())
  moviesDF.printSchema()

  //counting
  import org.apache.spark.sql.functions.col
  val genresCountDF = moviesDF.select(count(col("Major_Genre")))
  val genresCountDF_v2 = moviesDF.selectExpr("count(Major_Genre)")
  //Note we don't use moviesDF.selectExpr("count(col(Major_Genre)")
  //This will give us error
  val moviesDFCount = moviesDF.selectExpr("count(*)").show()
  genresCountDF.show()
  genresCountDF_v2.show()
}
