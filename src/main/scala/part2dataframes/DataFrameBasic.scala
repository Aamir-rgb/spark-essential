package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object DataFrameBasic  extends App {

  //Creating a spark session
  val spark = SparkSession.builder()
    .appName("DataFrame Basics")
    .config("spark.master","local")
    .getOrCreate()

  //Reading a Data Frame
  val firstDF = spark.read.format("json").option("inferSchema","true")
    .load("src/main/resources/data/cars.json")

  //showing a data frame
  firstDF.show()

  //printing schema
  firstDF.printSchema()

  //get rows
  firstDF.take(10).foreach(println)

  //spark types
  val longType = LongType

  //schema
  val carSchema = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", StringType),
      StructField("Origin", StringType)
    )
  )

  val carsDFSchema = firstDF.schema
  println(carsDFSchema)

  //Read a DF with your schema
  val carsDFWithSchema = spark.read.format("json").schema(carsDFSchema).load("src/main/resources/data/cars.json")

  //Create Rows By Hand
  val myRow = Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")

  //Create DF from tuples
  val cars = Seq(
    ("chevrolet chevelle malibu",18,8,307,130,3504,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15,8,350,165,3693,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18,8,318,150,3436,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16,8,304,150,3433,12.0,"1970-01-01","USA"),
    ("ford torino",17,8,302,140,3449,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15,8,429,198,4341,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14,8,454,220,4354,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14,8,440,215,4312,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14,8,455,225,4425,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15,8,390,190,3850,8.5,"1970-01-01","USA")
  )

  val manualCarsDF = spark.createDataFrame(cars)

  //Note DataFrames have schemas rows do not have

    //Create DF with implicits
  import spark.implicits._
  val manualCarsDFWithImplicits = cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "CountryOrigin")

  manualCarsDF.printSchema()
  manualCarsDFWithImplicits.printSchema()

  //Exercises
  val smartphones = Seq(
    ("Samsung", "Galaxy S10", "Android", 12),
    ("Apple", "iPhone X", "iOS", 13),
    ("Nokia", "3310", "THE BEST", 0)
  )

  val manualSmartPhones = spark.createDataFrame(smartphones)
  manualSmartPhones.printSchema()

  val manualSmartPhonesWithImplicits = smartphones.toDF("Make","Model","Platform","CameraMegapixels")
  manualSmartPhonesWithImplicits.printSchema()
  manualSmartPhonesWithImplicits.show()

  val moviesDF = spark.read.format("json").load("src/main/resources/data/movies.json")
  moviesDF.printSchema()
  println(s"The movies dataframe has ${moviesDF.count()} rows")
}
