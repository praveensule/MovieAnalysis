package com.movie.spark

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.junit.runner.RunWith
import org.scalatest.funspec.AnyFunSpec
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class IntegrationTesting extends AnyFunSpec
  with DataFrameComparer
  with SparkSessionTestWrapper {
  import spark.implicits._

  it("Integration of all ETL stages assures not failure") {
    LoadMovieData.main(Array("--files resources/main/reference.conf"))
    TransformMovieData.main(Array("--files resources/main/reference.conf"))
  }
}
