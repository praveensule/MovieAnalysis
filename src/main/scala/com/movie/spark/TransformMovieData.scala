package com.movie.spark

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object TransformMovieData {

  def main(args: Array[String]) {
    // Load configuration into Settings class
    val conf: Config = ConfigFactory.load()
    val settings: Settings = new Settings(conf)
    val appName = settings.appName + "- TransformMovieData"
    //System.setProperty("hadoop.home.dir", settings.winutilsPath)

    val spark = SparkSession.builder.master(settings.sparkClusterMode).appName(appName).getOrCreate()
    import spark.sqlContext.implicits._

    //Movie Genres data split
    val moviesRawPath=settings.outputFile+"/raw/movies"
    val moviesDF = spark.read.parquet(moviesRawPath)

    val moviesTransformedPath=settings.outputFile+"/transformed/movies"
    moviesDF
      .transform(TransformationUtils.splitGenres())
      .write.mode("overwrite").parquet(moviesTransformedPath)

    val ratingsRawPath=settings.outputFile+"/raw/ratings"
    val ratingsDF = spark.read.parquet(ratingsRawPath)
    val top10Movies=moviesDF.join(ratingsDF,"movieId")
      .groupBy("title")
      .agg(count("rating").as("num_ratings"),
        avg("rating").as("avg_rating"))
      .where(col("num_ratings") >= 5)
      .orderBy(col("avg_rating").desc)
      .limit(10)

    top10Movies.show(false)

    val top10MoviesTransformedPath=settings.outputFile+"/transformed/top10Movies"
    top10Movies
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .mode("overwrite")
      .save(top10MoviesTransformedPath)
  }
}
