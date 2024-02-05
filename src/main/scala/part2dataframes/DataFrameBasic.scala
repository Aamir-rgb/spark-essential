package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructField, _}

object DataFrameBasic  extends App {
  val spark = SparkSession.builder()
    .appName("DataFrame Practice")
    .config("spark.master","local")
    .getOrCreate()

  //Reading A Data Frame
  val firstDF = spark.read.format("json")
                .option("inferSchema","true")
    .load("src/main/resources/data/cars.json")

  //Showing the DataFrame
  firstDF.show()
  firstDF.printSchema()

  firstDF.take(10).foreach(println)

  //Spark Types
  val longType = LongType

  //schema
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  //Obtain A Schema
  val carsDFSchema = firstDF.schema
  println(carsDFSchema)

//Read a DataFrame With Your Own Schema
val carsDFWithSchema = spark.read.format("json")
    .schema(carsSchema)
    .option("inferSchema","true")
    .load("src/main/resources/data/cars.json")

  carsDFWithSchema.printSchema()

  //Create A Row
  val myRow = Row(12.0,8,307.0,130,18.0,"chevrolet chevelle malibu","USA",3504,"1970-01-01")

  //Create a DF From Tuples
//  val cars = Seq(
//  ("Name":"chevrolet chevelle malibu","Miles_per_Gallon":18.0,"Cylinders":8,"Displacement":307.0,"Horsepower":130,"Weight_in_lbs":3504,"Acceleration":12.0,"Year":"1970-01-01","Origin":"USA"),
//  ("Name":"buick skylark 320","Miles_per_Gallon":15.0,"Cylinders":8,"Displacement":350.0,"Horsepower":165,"Weight_in_lbs":3693,"Acceleration":11.5,"Year":"1970-01-01","Origin":"USA"),
//  ("Name":"plymouth satellite","Miles_per_Gallon":18.0,"Cylinders":8,"Displacement":318.0,"Horsepower":150,"Weight_in_lbs":3436,"Acceleration":11.0,"Year":"1970-01-01","Origin":"USA"),
////  ("Name":"amc rebel sst","Miles_per_Gallon":16.0,"Cylinders":8,"Displacement":304.0,"Horsepower":150,"Weight_in_lbs":3433,"Acceleration":12.0,"Year":"1970-01-01","Origin":"USA"),
////  ("Name":"ford torino","Miles_per_Gallon":17.0,"Cylinders":8,"Displacement":302.0,"Horsepower":140,"Weight_in_lbs":3449,"Acceleration":10.5,"Year":"1970-01-01","Origin":"USA"),
////  ("Name":"ford galaxie 500","Miles_per_Gallon":15.0,"Cylinders":8,"Displacement":429.0,"Horsepower":198,"Weight_in_lbs":4341,"Acceleration":10.0,"Year":"1970-01-01","Origin":"USA"),
////  ("Name":"chevrolet impala","Miles_per_Gallon":14.0,"Cylinders":8,"Displacement":454.0,"Horsepower":220,"Weight_in_lbs":4354,"Acceleration":9.0,"Year":"1970-01-01","Origin":"USA")
////  )

//  val manualCarsDf = spark.createDataFrame(cars)

}