package part2dataframes

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, mean, stddev}

object Aggregations extends App {
  //Create Spark Session
  val spark = SparkSession.builder()
             .appName("Aggregation and Groupings")
             .config("spark.master","local")
             .getOrCreate()
  val moviesDF = spark.read.option("inferSchema","true").json("src/main/resources/data/movies.json")
  println(moviesDF.count())
  moviesDF.printSchema()

  import org.apache.spark.sql.functions.{count,col}
  //counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre")))
  genresCountDF.show()
}
