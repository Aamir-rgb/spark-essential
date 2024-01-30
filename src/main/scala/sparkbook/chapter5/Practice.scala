package sparkbook.chapter5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{avg, count, countDistinct}

import scala.collection.immutable.Nil.distinct

object Practice extends App {

  val Spark = SparkSession.builder()
    .appName("Chapter 5")
    .config("spark.master","local")
    .getOrCreate()

  val df = Spark.read.format("json").load("src/main/resources/data/flight-data/json/2015-summary.json")
  //df.printSchema()
  import org.apache.spark.sql.types.{StructField,StructType,StringType,LongType}
  import org.apache.spark.sql.types.Metadata

  val myManualSchema = StructType(
    Array(
      StructField("DEST_COUNTRY_NAME",StringType,true),
      StructField("ORIGIN_COUNTRY_NAME",StringType,true),
      StructField("count",LongType,false,Metadata.fromJson("{\"hello\":\"world\"}"))
    )
  )

  val dfManual = Spark.read.format("json").schema(myManualSchema).load("src/main/resources/data/flight-data/json/2015-summary.json")
  dfManual.printSchema()

  import org.apache.spark.sql.functions.{col,column}
  col("SomeColumn")
  column("SomeColumnFrom")
  $"myColumn"
  'myColumn
  //Refer A Specific Column
  dfManual.col("count")
  dfManual.columns
  dfManual.first()

  //Expression Example
  import org.apache.spark.sql.functions.expr
  import org.apache.spark.sql.Row
  val myRow = Row("Hello",None,1,false)
  println(myRow(0))
  myRow(0)
  println(myRow.getString(0))

  //Create a DataFrame
  val dfCreate = Spark.read.format("json").load("src/main/resources/data/flight-data/json/2015-summary.json")
  dfCreate.createOrReplaceTempView("dfTable")

  val myManualSchemaAgain = new StructType(
    Array(
      new StructField("some",StringType,true),
      new StructField("col",StringType,true),
      new StructField("names",LongType,false)
    )
  )

  val myRows = Seq(Row("Hello",null,1L))
  val myRDD = Spark.sparkContext.parallelize(myRows)
  val myDF = Spark.createDataFrame(myRDD,myManualSchemaAgain)
  myDF.show()

//  val myDf = Seq(("Hello",2,1L)).to
    //.toDF("col1","col2","col3")

  //Select Expression
  df.select("DEST_COUNTRY_NAME","ORIGIN_COUNTRY_NAME").show(2)

  import org.apache.spark.sql.{SparkSession, functions}
  df.select(
//    df.col("DEST_COUNTRY_NAME"),
//    col("DEST_COUNTRY_NAME"),
//    column("DEST_COUNTRY_NAME"),
//    $"DEST_COUNTRY_NAME",
//    'DEST_COUNTRY_NAME,
    expr("DEST_COUNTRY_NAME"))
    .show(2)

  df.select(expr("DEST_COUNTRY_NAME AS DESTINATION")).show(5)
  df.selectExpr("DEST_COUNTRY_NAME as newColumnName","DEST_COUNTRY_NAME").show(2)
  df.selectExpr("*","DEST_COUNTRY_NAME=ORIGIN_COUNTRY_NAME").show(2)
  df.select(avg("count"),countDistinct("DEST_COUNTRY_NAME")).show(2)

  import org.apache.spark.sql.functions.lit
  df.select(expr("*"),lit(1).as("One")).show(2)
  df.withColumn("withinCountry",expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show(2)
  df.withColumn("Destination",expr("DEST_COUNTRY_NAME")).columns
  df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns
}
