package part2dataframes

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, mean, stddev}

object Aggregations extends App {

  val spark = SparkSession.builder()
    .config("spark.master","local")
    .getOrCreate()

  val moviesDF = spark.read.
    option("inferSchema","true")
    .json("src/main/resources/data/movies.json")

  //Aggregation Counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre"))) //All the values except null
  moviesDF.selectExpr("count(Major_Genre)").show()
  genresCountDF.show()
  moviesDF.select(count("*")).show() //count all the rows and will include Null
  moviesDF.select(countDistinct(col("Major_Genre"))).show()

  //Approximate Count
  moviesDF.select(approx_count_distinct(col("Major_Genre"))).show()

  //Min and Max
  val minRatingDF = moviesDF.select(functions.min(col("IMDB_Rating")))
  val maxRatingDF = moviesDF.select(functions.max(col("IMDB_Rating")))
  minRatingDF.show()
  maxRatingDF.show()

  //Sum And Average
  moviesDF.select(functions.sum(col("US_Gross"))).show()
  moviesDF.select(functions.avg(col("US_Gross"))).show()
  moviesDF.selectExpr("sum(US_Gross)").show()

  //Data Science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  ).show()

  //Grouping
  val countByGenreDF = moviesDF.groupBy(col("Major_Genre")).count()
  countByGenreDF.show()

  val avgRatingByGenreDF = moviesDF.groupBy(col("Major_Genre")).avg("IMDB_Rating")
  avgRatingByGenreDF.show()

  val aggregationByGenreDF = moviesDF.groupBy(col("Major_Genre")).agg(
    count("*").as("N_Movies"),
    avg("IMDB_rating").as("Avg_Rating")
  ).orderBy(col("Avg_Rating"))
  aggregationByGenreDF.show()

  //Exercises
  //1 sum up all the profits of All the movies in the DF
  import org.apache.spark.sql.functions._
  moviesDF
    .select((col("US_Gross")+col("Worldwide_Gross")+col("US_DVD_Sales")).as("Total_Profit"))
    .select(sum("Total_Profit"))
    .show()



  //2 Count how many distinct directors we have
  moviesDF.select(countDistinct(col("Director"))).show()
   moviesDF.select(count(col("Director"))).distinct().show()

  //3 show the mean and standard deviation of Us gross revenues for the movies
 moviesDF.agg(
    mean(col("US_Gross")),
    stddev(col("US_Gross"))
  ).show()

  //compute the average IMDB rating and the average us gross revenue per director
 moviesDF.groupBy(col("Director")).agg(
    avg("IMDB_Rating").as("Avg_IMDB_Rating"),
      avg("US_Gross").as("US_Gross_Average")
  ).show()

}
