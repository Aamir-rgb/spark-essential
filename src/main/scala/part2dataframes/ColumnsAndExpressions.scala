package part2dataframes

import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}
import part2dataframes.ColumnsAndExpressions.weightInKgExpression

import scala.reflect.internal.util.NoPosition.column
import scala.util.parsing.input.NoPosition.column

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns And Expressions")
    .config("spark.master","local")
    .getOrCreate()

  val carsDF = spark.read.option("inferSchema","true").json("src/main/resources/data/cars.json")
  carsDF.show()

  //Columns
  val firstColumn = carsDF.col("Name")
  val carsNameDF = carsDF.select(firstColumn)

  carsNameDF.show()

  //Various Select Columns
  // various select methods
  import spark.implicits._
  import org.apache.spark.sql.functions.column
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala Symbol, auto-converted to column
    $"Horsepower", // fancier interpolated string, returns a Column object
    expr("Origin") // EXPRESSION
  )

  //select with plain column names
  carsDF.select("Name","Year")

  //Expressions
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs")

  val carsWithWeightDF = carsDF.select(col("Name"),
    col("Weight_in_lbs"),
      weightInKgExpression.as("Weight_in_kg"))
  carsWithWeightDF.show()
}
