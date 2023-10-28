package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._
import part2dataframes.DataFrameBasic.spark

object DataSources {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Data Sources and Master")
      .config("spark.master", "local")
      .getOrCreate()
    val carsSchema = StructType(
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

    val carsDF = spark.read.format("json").schema(carsSchema) //enforce a schema
      .option("mode", "failFast")
      .option("path", "src/main/resources/data/cars.json").load()

    val carsDFWithOptionMap = spark.read.format("json").options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    )).load()
    //System.setProperty("hadoop.home.dir", "C:\\BigDataSetup\\hadoop")
    System.load("C:\\BigDataSetup\\hadoop\\bin\\hadoop.dll")
    System.load("C:\\BigDataSetup\\hadoop\\bin\\winutils.exe")
    //System.load(System.getenv("HADOOP_HOME") + "/")
    spark.read.text("src/main/resources/data/SampleTextFile.txt")
    carsDF.write
      .format("json")
      .mode(SaveMode.Overwrite)
      .save("src/main/resources/data/cars_dupe.json")

    carsDF.write
      .mode(SaveMode.Append)
      .save("src/main/resources/data/cars.parquet")

  }
}
