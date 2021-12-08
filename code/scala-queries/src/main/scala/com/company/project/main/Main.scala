package com.company.project
package main

import co.theasi.plotly.{Plot, draw, writer}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.util.Random

object Main extends App {

  val logRun = LoggerFactory.getLogger(getClass.getName)

  implicit val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

  // ejecutar las queries de una en una

//  val mostConsistentDrivers = ConsistencyProcessor(spark, 2012)
//
//  val mostConsistentDriversEver = ConsistencyProcessor.apply(spark)
//
//  mostConsistentDrivers.show(30)
//
//  mostConsistentDriversEver.show(30)
//
//
//  val mostDominantConstructor90s = ConstructorDominanceProcessor(spark, 1990 to 1999)
//
//  val mostDominantConstructor2020 = ConstructorDominanceProcessor(spark, 2020)
//
//  val mostDominantConstructorEver = ConstructorDominanceProcessor(spark)
//
//  mostDominantConstructor90s.show(false)
//
//  mostDominantConstructorEver.show(false)
//
//  mostDominantConstructor2020.show(false)

//  val analysis2021 = SeasonAnalysis(spark, 2021)

  val xs = (0 until 100)

  // Generate random y
  val ys = (0 until 100).map { i => i + 5.0 * Random.nextDouble }

  val p = Plot().withScatter(xs, ys)

  draw(p, "basic-scatter", writer.FileOptions(overwrite=true))

  spark.stop
}
