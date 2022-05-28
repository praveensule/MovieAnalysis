package com.movie.spark

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, lit, to_timestamp}
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TransformationUtilsSpec extends AnyFunSpec
  with DataFrameComparer
  with SparkSessionTestWrapper {

  import spark.implicits._

  it("splits a genres column to multiple rows") {

    val sourceDF = Seq(
      ("miguel","Adventure|Animation|Children|Comedy|Fantasy"),
      ("luisa","Action|Crime|Thriller")
    ).toDF("title","genres")

    val actualDF = sourceDF.transform(TransformationUtils.splitGenres())

    val expectedSchema = List(
      StructField("title", DataTypes.StringType, true),
      StructField("genres", DataTypes.StringType, true)
    )

    val expectedData = Seq(
      Row("miguel", "Adventure"),
      Row("miguel", "Animation"),
      Row("miguel", "Children"),
      Row("miguel", "Comedy"),
      Row("miguel", "Fantasy"),
      Row("luisa", "Action"),
      Row("luisa", "Crime"),
      Row("luisa", "Thriller")
    )
    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )
    assertSmallDataFrameEquality(actualDF, expectedDF)
  }

  it("cast integer field to timestamp field") {

    val sourceDF = Seq(
      ("miguel",964982703),
      ("luisa",964981247)
    ).toDF("title","timestamp")

    val actualDF = sourceDF.transform(TransformationUtils.convertIntToTimestamp())
    val expectedSchema = List(
      StructField("title", DataTypes.StringType, true),
      StructField("timestamp", DataTypes.TimestampType, false)
    )

    val expectedData = Seq(
      ("miguel","2000-07-30 20:45:03"),
      ("luisa","2000-07-30 20:20:47")
    ).toDF("title","timestamp")
      .withColumn("timestamp",to_timestamp(col("timestamp")))

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData.rdd.collect()),
      StructType(expectedSchema)
    )
    assertSmallDataFrameEquality(actualDF, expectedDF)
  }
}
