package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}

object DataSourcesRev  extends App {
  val spark = SparkSession.builder()
    .appName("Data Source Spark")
    .config("spark.master","local[*]")
    .getOrCreate()

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

  val carsDF = spark.read.format("json")
    .schema(carsSchema) //Enforce a Schema
    .option("mode","failFast") //dropMalormed //permissive
    .option("path","src/main/resources/data/cars.json")
    .load()

  carsDF.show()

  /**
   Reading a DataFrame
   -format
   -schema - > Optional
   */

    //Alternative
  var carsDFWithOptionMap = spark.read.format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" ->"src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    )).load()

  //Writing Data Frames
  //-format
  //-save mode overwrite,append,ignore,errorIfExists
  //-path
  //-zero or more options
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe.json")

  spark.read.format("json")
    .schema(carsSchema)
    .option("dateFormat","YYYY-MM-dd")
    .option("allowSingleQuotes","true")
    .option("compression","uncompressed") //bzip2,gzip,lz4,snappy,deflate
    .load("src/main/resources/data/cars.json")

  //CSV Flags
  val stockSchema = StructType(
    Array(
      StructField("symbol",StringType),
      StructField("date",DateType),
      StructField("price",DoubleType)
    )
  )

  spark.read
    .schema(stockSchema)
    .option("dateFormat","MMM dd YYYY")
    .option("header","true")
    .option("sep",",")
    .option("nullValue","")
    .csv("src/main/resources/data/stocks.csv")

  //Parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet")

  //Text Files
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  //Reading From  A Remote DB
  val employeeDF = spark.read.format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/postgres")
    .option("user","postgres")
    .option("password","admin")
    .option("dbtable","public.employees")
    .load()

  employeeDF.show()

  //Exercises
  val moviesDF = spark.read
    .json("src/main/resources/data/movies.json")
  moviesDF.printSchema()

//  moviesDF.write
//    .option("header","true")
//    .option("sep","\\t")
//    .option("nullValue","")
//    .csv("src/main/resources/data/movies.csv")

//  moviesDF.write
//    .mode(SaveMode.Overwrite)
//    .option("compression","SNAPPY")
//  .parquet("src/main/resources/data/movies.parquet")

  moviesDF.write.format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/postgres")
    .option("user","postgres")
    .option("password","admin")
    .option("dbtable","public.movies")
    .mode(SaveMode.Overwrite)
    .save()
}
