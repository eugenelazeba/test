/**
  * Created by eugene on 09.10.16.
  */

import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions._

import scala.collection.immutable.Range.Inclusive



object SparkSqlCsv {

  def main(args: Array[String]) {


    val conf = new SparkConf().setMaster("local[2]").setAppName("App Me")
    val sc = new org.apache.spark.SparkContext(conf)
    val sqlc = new org.apache.spark.sql.SQLContext(sc)

    val df = sqlc.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .load(args(0))

    df.createOrReplaceTempView("MY")
    /** First task
      * List of all airports with total number of planes for the whole period that arrived to each airport
      * */
//    val dfTaskFirst = sqlc.sql("SELECT (DEST), count(*) FROM MY group by (DEST)")
//
//    dfTaskFirst.repartition(1).write.mode(SaveMode.Append).format("com.databricks.spark.csv").option("header", "false").save("task1")
//
//
//    /** Second task
//      * Non-Zero difference in total number of planes that arrived to and left from the airport
//      * */
//    val dfGroupedByOriginMinus = sqlc.sql("SELECT (ORIGIN), -count(*) FROM MY group by (ORIGIN)")
//    val dfGroupedByDestination = sqlc.sql("SELECT (DEST), count(*) FROM MY group by (DEST)")
//    val dfUnionDestOrigin = dfGroupedByDestination.union(dfGroupedByOriginMinus)
//    val dfSumUnionDestOrigin = dfUnionDestOrigin.groupBy("DEST").sum("count(1)")
//
//    val setNameCountColumn = Seq("DEST", "QUANTITY")
//    val dfTaskSecondNotFiltered = dfSumUnionDestOrigin.toDF(setNameCountColumn: _*)
//
//    val dfTaskSecond = dfTaskSecondNotFiltered.filter("QUANTITY !=0")
//
//    dfTaskSecond.coalesce(1).write.mode(SaveMode.Append).format("com.databricks.spark.csv").option("header", "false").save("task2")

    /** Third task
      * Do the point 1 but sum number of planes separately per each week
      */
    val dfOriginDestFlDate = sqlc.sql("SELECT (FL_DATE),(ORIGIN),(DEST) FROM MY")
    val dfOriginDestFlDateWeeks = dfOriginDestFlDate.withColumn("FL_DATE", weekofyear(dfOriginDestFlDate("FL_DATE")))

    dfOriginDestFlDateWeeks.createOrReplaceTempView("MY")
    val lastWeekInYear = sqlc.sql("SELECT (FL_DATE) FROM MY")
    val lastWeekInYearSorted = lastWeekInYear.sort(desc("FL_DATE"))

    val weeks: Inclusive = 1 to lastWeekInYearSorted.first().getInt(0)
    weeks
      .map(i => getWeek(sqlc, "FL_DATE="+i, dfOriginDestFlDateWeeks))
      .foreach(printToFile)

    def printToFile(dataset: Dataset[Row]): Unit = {
      dataset.repartition(1).write.mode(SaveMode.Append).format("com.databricks.spark.csv").option("header", "false").save("task3")
    }

    def getWeek(sqlc: SQLContext, week: String, dfOriginDestFlDateWeeks: DataFrame) = {
      val dfOriginDestFlDateWeek = dfOriginDestFlDateWeeks.filter(week)

      dfOriginDestFlDateWeek.createOrReplaceTempView("MY")

      val weekDest = dfOriginDestFlDateWeek.select("DEST").groupBy("DEST").count()
      val weekOrigin = sqlc.sql("SELECT (ORIGIN), -count(*) FROM MY group by (ORIGIN)")
      val dfTaskThird = weekDest.union(weekOrigin.withColumn("(-count(1))", lit(0)))
      dfTaskThird
    }
  }

}
