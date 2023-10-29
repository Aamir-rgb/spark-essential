package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import part2dataframes.Joins.bandsDF

object Joins extends App {

  val spark = SparkSession.builder()
    .config("spark.master","local")
    .getOrCreate()

  val guitarsDF = spark.read.option("inferSchema","true").json("src/main/resources/data/guitars.json")

  val guitaristDF = spark.read.option("inferSchema","true").json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read.option("inferSchema","true").json("src/main/resources/data/bands.json")

  val joinCondition = guitaristDF.col("band") === bandsDF.col("id")

  val guitaristBandsDF = guitaristDF.join(bandsDF,joinCondition,"inner")

  guitaristBandsDF.show()

  //outer joins
  //left outer join = inner join +left table with nulls where data is missing
  guitaristDF.join(bandsDF,joinCondition,"left_outer").show()

  //right outer join = everything in the inner join + all the rows from right table
  guitaristDF.join(bandsDF,joinCondition,"right_outer").show()

  //full outer join everything in the inner join plus all the rows in both tables with nulls where data is missing
  guitaristDF.join(bandsDF,joinCondition,"outer").show()

  //semi joins everything in the left DF for which there is a row in the right DF satisfying the condition
  guitaristDF.join(bandsDF,joinCondition,"left_semi").show()

  //anti joins  everything in the left DF for which there is no row in the right DF satisfying the condition
  guitaristDF.join(bandsDF,joinCondition,"left_anti").show()

  //things to bear in mind
  // guitaristBandsDF.select("id","name").show() //this crashes

  //Option 1
  guitaristDF.join(bandsDF.withColumnRenamed("id","band"),"band").show()

  //Option 2 drop the dup columns
  guitaristBandsDF.drop(bandsDF.col("id")).show()

  //Option 3 rename the offending columns
  val bandsModDF = bandsDF.withColumnRenamed("id","bandId")
  guitaristDF.join(bandsModDF,guitaristDF.col("band") === bandsModDF.col("bandId"),"left_anti").show()

  //Using Columns Types
  guitaristDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))

}
