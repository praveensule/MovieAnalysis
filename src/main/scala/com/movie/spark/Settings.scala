package com.movie.spark

import com.typesafe.config.Config
import scala.collection.JavaConverters._

/**
 * Settings class for configuration.
 */
class Settings(config: Config) extends Serializable {

    val appName = config.getString("appName")
    val moviesInputFile = config.getString("file.moviesInputFile")
    val ratingsInputFile = config.getString("file.ratingsInputFile")
    val linksInputFile = config.getString("file.linksInputFile")
    val tagsInputFile = config.getString("file.tagsInputFile")
    val inputFile = config.getString("file.input")
    val outputFile = config.getString("file.output")
    val sparkClusterMode = config.getString("sparkClusterMode")

}