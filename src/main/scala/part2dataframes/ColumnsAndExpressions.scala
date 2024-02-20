package part2dataframes

import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, column, expr}

import scala.reflect.internal.util.NoPosition.column
import scala.util.parsing.input.NoPosition.column

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns And Expressions")
    .config("spark.master","local")
    .getOrCreate()

  var carsDF = spark.read.
                 option("inferSchema","true")
                .json("src/main/resources/data/cars.json")

  carsDF.show()

  val firstColumn = carsDF.col("Name")
  val carsNameDf = carsDF.select(firstColumn)
  carsNameDf.show()
import spark.implicits._
  carsDF.select(
    col("Name"),col("Acceleration"),col("Weight_in_lbs"),
     expr("Origin"),'Year,col("Horsepower")
  )

//Expressions
  val SimplestExpression = carsDF.col("Weight_in_lbs")
  val WeightInKgExpression = carsDF.col("Weight_in_lbs")/2.2

  val carsWithWeightDF = carsDF.select(col("Name"),col("Weight_in_lbs"),
    WeightInKgExpression as ("Weight_in_kg"))
  carsWithWeightDF.show(23)

  val carsWitSelectEXprWeightDF = carsDF.selectExpr(
    "Weight_in_lbs","Weight_in_lbs/2.2")
  //DF Processing
  carsDF = carsDF.withColumn("Weight_in_pounds",col("Weight_in_lbs"))
  carsDF.printSchema()
  carsDF = carsDF.withColumnRenamed("Weight_in_pounds","Weight_in_lbs")
  carsDF.printSchema()
  carsDF.drop("Weight_in_lbs")
  val carsDFDrop = carsDF.drop("Weight_in_pounds")
  carsDFDrop.printSchema()

  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2= carsDF.where(col("Origin") =!= "USA")

  val americanCarsDF = carsDF.filter("Origin = 'USA'")

  //Chain Filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  //Unioning Adding More Rows
  val moreCarsDF = spark.read.option("inferSchema","true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = moreCarsDF.union(moreCarsDF)

  //Distinct Values
  val allCountriesDF = carsDF.select("Origin").distinct()

  allCountriesDF.show()


  //Exercises
  //Read the movies DataFrame
  val moviesDF = spark.read.option("inferSchema","true").json("src/main/resources/data/movies.json")
  val totalMoviesGrossDF = moviesDF.selectExpr("Title","US_Gross","Worldwide_Gross","US_DVD_Sales","US_Gross+Worldwide_Gross as Total_Gross")
  totalMoviesGrossDF.show()
  val comedyMovies = moviesDF.where("Major_Genre = 'Comedy' and IMDB_Rating > 6")
    comedyMovies.show()
}
