package com.company.project.main
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType


object ConstructorDominanceProcessor {

  def apply(spark: SparkSession, seasons: Range.Inclusive): DataFrame = {
    this.apply(spark, seasons: _*)
  }


  def apply(spark: SparkSession, seasons: Int *): DataFrame = {

    val lastRace = Window.partitionBy("year")

    val lastRaces = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .load("data/races.csv")
      // filtro las temporadas que me interesan
      .withColumn("year", col("year").cast(IntegerType))
      .where(col("year").isInCollection(seasons))
      .withColumn("round", col("round").cast(IntegerType))
      // me quedo unicamente con las ultimas carreras de cada temporada
      .withColumn("max", max(col("round")).over(lastRace))
      .where(col("round") === col("max"))
      .select("raceId", "year")

    computeDominance(spark, lastRaces)
  }

  def apply(spark: SparkSession): DataFrame = {

    val lastRace = Window.partitionBy("year")

    val lastRaces = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .load("data/races.csv")
      // filtro las temporadas que me interesan
      .withColumn("year", col("year").cast(IntegerType))
      .withColumn("round", col("round").cast(IntegerType))
      // me quedo unicamente con las ultimas carreras de cada temporada
      .withColumn("max", max(col("round")).over(lastRace))
      .where(col("round") === col("max"))
      .select("raceId", "year")

    computeDominance(spark, lastRaces)
  }

  private def computeDominance(spark: SparkSession, lastRaces: DataFrame): DataFrame = {
    val constructorWindow = Window.partitionBy("constructorId")

    val constructorWinners = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .load("data/constructor_standings.csv")
      .join(lastRaces, Seq("raceId"), "right")
      // me quedo con los primeros en la clasificacion de las ultimas carreras
      .where(col("position") === 1)
      .select("constructorId", "wins", "year")
      // cuento los mundiales ganados y sumo las victorias
      .withColumn("totalChampWins", count(col("constructorId")).over(constructorWindow))
      .withColumn("totalRaceWins", sum(col("wins")).over(constructorWindow).cast(IntegerType))
      .drop("wins")

    val constructors = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .load("data/constructors.csv")
      .select("constructorId", "name")

    constructorWinners
      // añado los nombres de los constructores
      .drop("year")
      .dropDuplicates("constructorId")
      .orderBy(col("totalChampWins").desc, col("totalRaceWins").desc)
      .join(constructors, "constructorId")
      .drop("constructorId")

  }
}
