package sparkbook.chapter1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc;


object Practice extends App {

  val spark = SparkSession.builder()
    .config("spark.master","local")
    .getOrCreate()

  //Example 1
  val myRange = spark.range(1000).toDF("number")

  myRange.printSchema()

  //Example 2
  val divisBy2 = myRange.where("number%2 == 0")
  divisBy2.show()

  //End To End Example
  val flightData2015 = spark.read
                       .option("inferSchema","true")
                       .option("header","true")
                       .csv("src/main/resources/data/flight-data/2015-summary.csv")
  flightData2015.show()

  flightData2015.take(3).foreach(println)

  flightData2015.sort("count").explain()

  spark.conf.set("spark.sql.shuffle.partitions", "5")
  flightData2015.sort("count").take(2).foreach(println)

  //Convert DataFrame Into Table or View
  flightData2015.createOrReplaceTempView("flight_data_2015")

  val sqlWay = spark.sql("""select dest_country_name from flight_data_2015 group by dest_country_name""")
  sqlWay.show()
  sqlWay.explain()

  val dataFrameWay = flightData2015.groupBy("dest_country_name").count()
  dataFrameWay.explain()

  spark.sql("SELECT max(count) from flight_data_2015").take(1).foreach(println)
  import org.apache.spark.sql.functions.max
  flightData2015.select(max("count")).take(1).foreach(println)

  val maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")

maxSql.show()

  flightData2015
    .groupBy("dest_country_name")
    .sum("count")
    .withColumnRenamed("sum(count)","destination_total")
    .sort(desc("destination_total"))
    .limit(5)
    .explain()

}
