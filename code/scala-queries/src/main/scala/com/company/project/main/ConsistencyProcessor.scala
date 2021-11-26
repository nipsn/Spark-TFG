package com.company.project.main
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object ConsistencyProcessor {

  def apply(spark: SparkSession, seasons: Range.Inclusive): DataFrame = {
    this.apply(spark, seasons: _*)
  }


  def apply(spark: SparkSession, seasons: Int *): DataFrame = {
    val races = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .load("data/races.csv")
      .withColumn("year", col("year").cast(IntegerType))
      // para filtrar por temporadas hay que hacerlo por la columna "year"
      .where(col("year").isInCollection(seasons))

    computeConsistency(spark, races)
  }

  def apply(spark: SparkSession): DataFrame = {
    val races = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .load("data/races.csv")
      .withColumn("year", col("year").cast(IntegerType))
      .where(col("year") >= 1996)

    computeConsistency(spark, races)
  }

  private def computeConsistency(spark: SparkSession, races: DataFrame): DataFrame = {
    import spark.implicits._

    val lapTimeToMs = (time: String) => {
      // TODO: algunos "laptimes" no hacen match en esta regex en el año 2009
      val regex = """([0-9]|[0-9][0-9]):([0-9][0-9])\.([0-9][0-9][0-9])""".r
      time match {
        case regex(min,sec,ms) => min.toInt * 60 * 1000 + sec.toInt * 1000 + ms.toInt
        case "\\N" => 180000
      }
    }: Long

    val lapTimeToMsUDF = udf(lapTimeToMs)
    spark.udf.register("lapTimeToMs", lapTimeToMsUDF)

    val msToLapTime = (time: Long) => {
      val mins = time / 60000
      val secs = (time - mins*60000)/1000
      val ms = time - mins*60000 - secs*1000

      val formattedSecs = if((secs / 10).toInt == 0) "0" + secs else secs
      // if ms = 00x -> "0"+"0"+x . if ms = 0xx -> "0"+ms
      val formattedMs = if((ms / 100).toInt == 0) "0" + (if((ms / 10).toInt == 0) "0" + ms else ms) else ms
      mins + ":" + formattedSecs + "." + formattedMs
    }: String

    val msToLapTimeUDF = udf(msToLapTime)
    spark.udf.register("msToLapTime", msToLapTimeUDF)


    val driverWindow = Window.partitionBy("driverId")

    val avg_lap_times = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .load("data/lap_times.csv")
      .withColumnRenamed("time", "lapTime")
      // filtro las vueltas de las carreras en el periodo de tiempo dado
      .join(races, Seq("raceId"), "right")
      .withColumn("milliseconds", col("milliseconds").cast(IntegerType))
      // media de tiempos de vuelta por piloto
      .withColumn("avgMs", avg(col("milliseconds")).over(driverWindow))
      .dropDuplicates("driverId")
      .select("driverId", "avgMs")


    val drivers = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .load("data/drivers.csv")

    val seasonWindow = Window.partitionBy("year")

    val lapCount = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .load("data/lap_times.csv")
      .join(races, Seq("raceId"), "right")
      .withColumn("lapsPerDriver", count(col("lap")).over(driverWindow))

    // saco la media de vueltas dadas en este periodo de tiempo
    val (distinctDrivers, allLaps) = lapCount
      .agg(
        countDistinct("driverID"),
        count(col("lap"))
      ).as[(BigInt, BigInt)]
      .collect()(0)

    val avgLapsThisPeriod = allLaps.toInt / distinctDrivers.toInt


    //filtrar con isInCollection es igual a hacer un join left-anti y mejora el rendimiento
    val experiencedDrivers = lapCount
      // filtro los pilotos mas experimentados
      .where(col("lapsPerDriver") >= avgLapsThisPeriod)
      .select("driverId")
      .distinct()
      .as[String]
      .collect()

    spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .load("data/results.csv")
      // filtro por temporada
      .join(races, Seq("raceId"), "right")
      .na.drop(Seq("fastestLapTime"))
      // paso la vuelta rapida de tiempo por vuelta a ms
      .withColumn("fastestLapTimeMs", lapTimeToMsUDF(col("fastestLapTime")))
      // saco la media de vueltas rapidas
      .withColumn("avgFastestLapMs", avg(col("fastestLapTimeMs")).over(driverWindow))
      .dropDuplicates("driverId")
      .join(avg_lap_times, Seq("driverId"), "left")
      // saco el diferencial
      .withColumn("diffLapTimes", abs('avgMs - 'avgFastestLapMs).cast(IntegerType))
      // vuelvo a pasar a tiempo de vuelta
      .withColumn("avgDiff", msToLapTimeUDF(col("diffLapTimes").cast(IntegerType)))
      // filtro pilotos "experimentados"
      .where(col("driverId").isInCollection(experiencedDrivers))
      // concateno el nombre y apellido de los pilotos
      .join(drivers, "driverId")
      .withColumn("driver", concat(col("forename"), lit(" "), col("surname")))
      .select("driver", "avgDiff")
      .orderBy("avgDiff")

  }
}
