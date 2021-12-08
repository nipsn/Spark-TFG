package com.company.project.main

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object SeasonAnalysis {
  def apply(spark: SparkSession, season: Int): DataFrame = {

    val driverWindow = Window.partitionBy("driverId")
    val seasonWindow = Window.partitionBy("year")
    val driverRaceWindow = Window.partitionBy("driverId", "raceId")
    val raceDriverLapWindow = Window.partitionBy("driverId", "raceId").orderBy("lap")


    val races = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .load("data/races.csv")
      .select("raceId", "year")
      .where(col("year") === season)

    val drivers = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .load("data/drivers.csv")

    val pitStops = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .load("data/pit_stops.csv")
      .withColumn("lap", (col("lap") - 1).cast(IntegerType))
      .join(races, "raceId")
      .select("raceId", "driverId", "lap")

    val overtakes = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .load("data/lap_times.csv")

      .withColumn("position", col("position").cast(IntegerType))
      .withColumn("lap", col("lap").cast(IntegerType))

      .join(races, "raceId")
      .join(pitStops, Seq("raceId", "driverId", "lap"), "left")

      .withColumn("positionNextLap", lead(col("position"), 1).over(raceDriverLapWindow))
      .withColumn("positionsGainedLap", when(col("positionNextLap") < col("position") , abs(col("position") - col("positionNextLap"))).otherwise(0))
      .withColumn("positionsLostLap", when(col("positionNextLap") > col("position"), abs(col("position") - col("positionNextLap"))).otherwise(0))

      .where(col("lap") === 1)
      .select(
        col("raceId"),
        col("driverId"),
        sum(col("positionsGainedLap")).over(driverRaceWindow).as("positionsGained"),
        sum(col("positionsLostLap")).over(driverRaceWindow).as("positionsLost")
      )

    spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .load("data/results.csv")

      .withColumn("position", col("position").cast(IntegerType))
      .withColumn("grid", col("grid").cast(IntegerType))
      .withColumn("points", col("points").cast(IntegerType))

      .join(races, "raceId")
      .join(overtakes, Seq("raceId", "driverId"), "left")
      .join(drivers, "driverId")

      .withColumn("podium", when(col("position") === 1 || col("position") === 2 ||col("position") === 3, lit(1)).otherwise(lit(0)))
      .withColumn("averagePoints", round(avg(col("points")).over(driverWindow), 2))
      .withColumn("maxAvgPoints", max(col("averagePoints")).over(seasonWindow))

      .select(
        col("code"),
        sum(col("points")).over(driverWindow).as("champPoints"),
        col("averagePoints"),
        round(col("averagePoints") / col("maxAvgPoints")).as("pointPercent"),
        sum(col("podium")).over(driverWindow).as("totalPodiums"),
        round(sum(col("podium")).over(driverWindow) / count(col("podium")).over(driverWindow), 2).as("podiumPercent"),
        round(avg(col("position") - col("grid")).over(driverWindow), 2).as("positionDelta"),
        round(avg(col("positionsLost")).over(driverWindow), 2).as("avgPositionsLost"),
        round(avg(col("positionsGained")).over(driverWindow), 2).as("avgPositionsWon"),
        sum(col("positionsLost")).over(driverWindow).as("totalPositionsLost"),
        sum(col("positionsGained")).over(driverWindow).as("totalPositionsWon")
      )

      .dropDuplicates(Seq("code"))
      .sort(col("champPoints").desc)



  }
}
