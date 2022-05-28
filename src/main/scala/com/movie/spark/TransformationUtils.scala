package com.movie.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode, split}
import org.apache.spark.sql.types.TimestampType

object TransformationUtils {

  def splitGenres()(movieDF: DataFrame) = {
    movieDF.withColumn("genres", explode(split(col("genres"), "\\|")))
  }

  def convertIntToTimestamp()(df: DataFrame) = {
    df.withColumn("timestamp", col("timestamp").cast(TimestampType))
  }
}
