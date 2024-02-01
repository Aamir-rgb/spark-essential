package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructField, StructType}

object DataFrameExercises extends App{

  val spark = SparkSession.builder().appName("DataFrame Exercises")
    .config("spark.master","local[*]")
    .getOrCreate()


  import spark.implicits._
  val smartphones = Seq(("Nokia","N-96","12.5","32"),("Apple","iphone-5","12","32"),("Samsung","A=71","14.5","64"))
  val mobileDataFrame = smartphones toDF("Make", "Model", "Screen Dimension", "Camera Megapixel")
  mobileDataFrame.show()
  mobileDataFrame.printSchema()

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")
  moviesDF.show()
  moviesDF.printSchema()
}
