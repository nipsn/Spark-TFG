package com.company.project
package main

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
object Main extends App {

  val logRun = LoggerFactory.getLogger(getClass.getName)

  implicit val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

  // ejecutar las queries de una en una

  val mostConsistentDrivers = ConsistencyProcessor(spark, 2012)

  val mostConsistentDriversEver = ConsistencyProcessor.apply(spark)

  mostConsistentDrivers.show(30)

  mostConsistentDriversEver.show(30)


  val mostDominantConstructor90s = ConstructorDominanceProcessor(spark, 1990 to 1999)

  val mostDominantConstructor2020 = ConstructorDominanceProcessor(spark, 2020)

  val mostDominantConstructorEver = ConstructorDominanceProcessor(spark)

  mostDominantConstructor90s.show(false)

  mostDominantConstructorEver.show(false)

  mostDominantConstructor2020.show(false)


  spark.stop
}
