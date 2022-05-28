package com.movie.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType


object LoadMovieData {

  def main(args: Array[String]) {
    // Load configuration into Settings class
    val conf: Config = ConfigFactory.load()
    val settings: Settings = new Settings(conf)
    val appName= settings.appName+"- LoadMovieData"
    //System.setProperty("hadoop.home.dir", settings.winutilsPath)

    val spark = SparkSession.builder.master(settings.sparkClusterMode).appName(appName).getOrCreate()
    import spark.sqlContext.implicits._

    //Read input files
    val ratingsDF= spark.read.option("header", "true")
      .option("inferSchema","true")
      .csv(settings.ratingsInputFile)

    val moviesDF= spark.read.option("header", "true")
                    .option("inferSchema","true")
                    .csv(settings.moviesInputFile)

    val tagsDF= spark.read.option("header", "true")
      .option("inferSchema","true")
      .csv(settings.tagsInputFile)

    //Write data in to parquet files
    val outputPath= settings.outputFile+"/raw"
    ratingsDF.transform(TransformationUtils.convertIntToTimestamp())
      .withColumn("year", year(col("timestamp")))
      .write.mode("overwrite")
      .partitionBy("year").parquet(outputPath+"/ratings")

    moviesDF.write
      .mode("overwrite")
      .parquet(outputPath+"/movies")

    tagsDF.transform(TransformationUtils.convertIntToTimestamp()).write
      .mode("overwrite")
      .parquet(outputPath+"/tags")

    spark.stop()
  }


}