package com.company.project
package main

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object Main extends App {

  val logRun = LoggerFactory.getLogger(getClass.getName)
  implicit val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

  val mostConsistentDrivers = ConsistencyProcessor(spark, 2012)

  mostConsistentDrivers.show(30)

  spark.stop
}
