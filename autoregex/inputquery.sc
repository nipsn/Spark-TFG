import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val spark = SparkSession
      .builder()
      .config("spark.sql.shuffle.partitions", 4)
      .master("local[4]")
      .getOrCreate()

import spark.implicits._

val m = Map(a -> "b", 
            b -> "c").withDefaultValue("na")

val milista = List("a",
    "b")

val lastRace = Window.partitionBy("year")

val lastRaces = spark.read.format("csv")
    .option("header", "true")
    .option("sep", ",")
    .load("../data/races.csv")
    .where($"year" >= 1990 && 'year <= 1999 || 'year <= 1998)
    .withColumn("round", col("round").cast(IntegerType))
    .withColumn("max", max(col("round")).over(lastRace))
    .withColumn("test", when(col("round") === col("max") || col("round") === 1, 23))
    .select("raceId", "year")

val constructors = Window.partitionBy("constructorId")

val constructorWinners = spark.read.format("csv")
    .option("header", "true")
    .option("sep", ",")
    .load("../data/constructor_standings.csv")
    .join(lastRaces, Seq("raceId"), "right")
    .where(col("position") === 1)
    .select("constructorId", "wins", "year")
    .withColumn("totalChampWins", count(col("constructorId")).over(constructors))
    .withColumn("totalRaceWins", sum(col("wins")).over(constructors).cast(IntegerType))
    .drop("wins")

val constructors = spark.read.format("csv")
    .option("header", "true")
    .option("sep", ",")
    .load("../data/constructors.csv")
    .select("constructorId", "name")

val mostDominantConstructor = constructorWinners
    .drop("year")
    .dropDuplicates("constructorId")
    .orderBy(col("totalChampWins").desc, col("totalRaceWins").desc)
    .join(constructors, "constructorId")
    .drop("constructorId")

