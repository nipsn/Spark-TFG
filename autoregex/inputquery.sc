import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

val spark = SparkSession
      .builder()
      .config("spark.sql.shuffle.partitions", 4)
      .master("local[4]")
      .getOrCreate()

import spark.implicits._

val m = Map(a -> "b", 
            b -> "c")

val milista = List("a",
    "b")

val lastRace = Window.partitionBy("year")

val constructorWindow = Window.partitionBy("constructorId")

val lastRaces = spark.read.format("csv")
    .option("header", "true")
    .option("sep", ",")
    .load("../data/races.csv")
    .where(col("year") >= 1990 && col("year") <= 1999)
    .withColumn("round", col("round").cast(IntegerType))
    .withColumn("max", max(col("round")).over(lastRace))
    .where(col("round") === col("max"))
    .select("raceId", "year")

val constructorMap = spark.read.format("csv")
    .option("header", "true")
    .option("sep", ",")
    .load("../data/constructors.csv")
    .select("constructorId", "name")

val results = spark.read.format("csv")
    .option("header", "true")
    .option("sep", ",")
    .load("../data/constructor_standings.csv")
    .join(lastRaces, Seq("raceId"), "right")
    .where(col("position") === 1)
    .select("constructorId", "wins", "year")
    .withColumn("totalChampWins", 
                count(col("constructorId")).over(constructorWindow).cast(IntegerType))
    .withColumn("totalRaceWins", 
                sum(col("wins")).over(constructorWindow).cast(IntegerType))
    .dropDuplicates("constructorId")
    .join(constructorMap, "constructorId")
    .select("totalChampWins", "totalRaceWins", "name")
    .orderBy(col("totalChampWins").desc, col("totalRaceWins").desc)


