package fr.esgi.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WikiPageCounts {

  case class WikiPageCount(projectName : String, pageUrl : String, nbRequests : Integer)

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("pagecounts").master("local[*]").getOrCreate()
    import spark.implicits._

    spark.read
      .option("delimiter", " ")
      .schema("projectName String, pageUrl String, nbRequests Integer")
      .csv("src/main/resources/wikipedia-pagecounts/*")
      .as[WikiPageCount]
      .groupBy("pageUrl")
      .sum("nbRequests")
      .orderBy(col("sum(nbRequests)").desc)
      .show()
  }
}
