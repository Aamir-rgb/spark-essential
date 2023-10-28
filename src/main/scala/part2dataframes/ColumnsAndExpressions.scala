package part2dataframes

import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

import scala.reflect.internal.util.NoPosition.column
import scala.util.parsing.input.NoPosition.column

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns And Expressions")
    .config("spark.master","local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/cars.json")

  //Columns
  val firstColumn = carsDF.col("Name")

  //Selecting
  val carNamesDF = carsDF.select(firstColumn)

  carNamesDF.show()

  //various select methods
  import spark.implicits._
  import org.apache.spark.sql.functions.column

  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala Symbol, auto-converted to column
    $"Horsepower",
    // fancier interpolated string, returns a Column object
    expr("Origin"))

    // EXPRESSION
    val simplesExpression = carsDF.col("Weight_in_lbs")
    val weightInKgExpression = carsDF.col("Weight_in_lbs")/2.2

    val carsWithWeightDF = carsDF.select(col("Name"),
      col("Weight_in_lbs"),
      weightInKgExpression.as("Weight_in_kg"),
      expr("Weight_in_lbs/2.2").as("Weight_in_kg_2")
    )
    val carWithSelectExprWeightDF = carsDF.selectExpr(
      "Name",
      "Weight_in_lbs",
      "Weight_in_lbs/2.2"
  )

  //DF Processing

  //Adding a column
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3",col("Weight_in_lbs")/2.2)

  //Renaming a Column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs","Weight in pounds")

  //Careful  with Column Names
  carsWithColumnRenamed.selectExpr("'Weight in pounds'")

  //Remove a Column
  carsWithColumnRenamed.drop("Cylinders","Displacement")

  //Filtering
  val europeanCarsDF =  carsDF.filter(col("Origin") =!="USA")

  val europeanCarsDF2 =  carsDF.where(col("Origin") =!="USA")

  //Filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")

  //Chain Filters
  val americanPowerfulCarsDF = carsDF.filter("Origin = 'USA'").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter(col("Origin") ==="USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  //Unioning - Adding More Rows
  val moreCarsDF = spark.read.option("inferSchema","true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF)

  //Distinct Value
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()

  //Exercise




  val moviesDF = spark.read.format("json").option("inferSchema","true").load("src/main/resources/data/movies.json")

  //1. Read the movies DF and select 2 columns of your choice
  val moviesDF2Columns = moviesDF.select(col("Title"),column("Worldwide_Gross"))
  moviesDF2Columns.show()
  //2. Create another column summed up the total profit of the movies = US_Gross + Worldwide_Gross
  val totalProfitColumn = moviesDF.withColumn("Total_Profit",col("US_Gross")+col("Worldwide_Gross"))
  totalProfitColumn.show()
  //3. Select all the comedy movies in the major Genre with IMDB Rating above 6
  val comedyMoviesDF = moviesDF.filter("Major_Genre = 'Comedy' and IMDB_Rating > 6")
  comedyMoviesDF.show()
}
