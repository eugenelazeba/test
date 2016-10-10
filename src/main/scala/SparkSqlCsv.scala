/**
  * Created by eugene on 09.10.16.
  */

import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}



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
    val dfTaskFirst = sqlc.sql("SELECT (DEST), count(*) FROM MY group by (DEST)")

    dfTaskFirst.repartition(1).write.mode(SaveMode.Append).format("com.databricks.spark.csv").option("header", "false").save("task1")


    /** Second task
      * Non-Zero difference in total number of planes that arrived to and left from the airport
      * */
    val dfGroupedByOriginMinus = sqlc.sql("SELECT (ORIGIN), -count(*) FROM MY group by (ORIGIN)")
    val dfGroupedByDestination = sqlc.sql("SELECT (DEST), count(*) FROM MY group by (DEST)")
    val dfUnionDestOrigin = dfGroupedByDestination.union(dfGroupedByOriginMinus)
    val dfSumUnionDestOrigin = dfUnionDestOrigin.groupBy("DEST").sum("count(1)")

    val setNameCountColumn = Seq("DEST", "QUANTITY")
    val dfTaskSecondNotFiltered = dfSumUnionDestOrigin.toDF(setNameCountColumn: _*)

    val dfTaskSecond = dfTaskSecondNotFiltered.filter("QUANTITY !=0")

    dfTaskSecond.coalesce(1).write.mode(SaveMode.Append).format("com.databricks.spark.csv").option("header", "false").save("task2")

    /** Third task
      * Do the point 1 but sum number of planes separately per each week
      */
    val dfOriginDestFlDate = sqlc.sql("SELECT (FL_DATE),(ORIGIN),(DEST) FROM MY")
    val dfOriginDestFlDateWeeks = dfOriginDestFlDate.withColumn("FL_DATE", weekofyear(dfOriginDestFlDate("FL_DATE")))

    dfOriginDestFlDateWeeks.createOrReplaceTempView("MY")
    val lastWeekInYear = sqlc.sql("SELECT (FL_DATE) FROM MY")
    val lastWeekInYearSorted = lastWeekInYear.sort(desc("FL_DATE"))

    for  (i<-1 to lastWeekInYearSorted.first().getInt(0)) {
            getWeek(sqlc, "FL_DATE="+i, dfOriginDestFlDateWeeks)}


    def getWeek(sqlc: SQLContext, week: String, dfOriginDestFlDateWeeks: DataFrame): Unit = {
      val dfOriginDestFlDateWeek = dfOriginDestFlDateWeeks.filter(week)

      dfOriginDestFlDateWeek.createOrReplaceTempView("MY")

      val week1Dest = dfOriginDestFlDateWeek.select("DEST").groupBy("DEST").count()
      val week1Origin = sqlc.sql("SELECT (ORIGIN), -count(*) FROM MY group by (ORIGIN)")
      val weeks = week1Dest.union(week1Origin.withColumn("(-count(1))", lit(0)))

      weeks.repartition(1).write.mode(SaveMode.Append).format("com.databricks.spark.csv").option("header", "false").save("task3")

    }
  }

}
