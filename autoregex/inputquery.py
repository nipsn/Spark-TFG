from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window

spark = SparkSession.builder\
      .config("spark.sql.shuffle.partitions", 4)\
      .master("local[4]")\
      .getOrCreate()\


m = {a : "b", 
            b : "c"}

milista = ["a",
    "b"]

lastRace = Window.partitionBy("year")\

constructorWindow = Window.partitionBy("constructorId")\

lastRaces = spark.read.format("csv")\
    .option("header", "True")\
    .option("sep", ",")\
    .load("../data/races.csv")\
    .where((F.col("year") >= 1990) & (F.col("year") <= 1999))\
    .withColumn("round", F.col("round").cast(T.IntegerType()))\
    .withColumn("max", F.max(F.col("round")).over(lastRace))\
    .where(F.col("round") == F.col("max"))\
    .select("raceId", "year")\

constructorMap = spark.read.format("csv")\
    .option("header", "True")\
    .option("sep", ",")\
    .load("../data/constructors.csv")\
    .select("constructorId", "name")\

results = spark.read.format("csv")\
    .option("header", "True")\
    .option("sep", ",")\
    .load("../data/constructor_standings.csv")\
    .join(lastRaces, ["raceId"], "right")\
    .where(F.col("position") == 1)\
    .select("constructorId", "wins", "year")\
    .withColumn("totalChampWins", 
                F.count(F.col("constructorId")).over(constructorWindow).cast(T.IntegerType()))\
    .withColumn("totalRaceWins", 
                F.sum(F.col("wins")).over(constructorWindow).cast(T.IntegerType()))\
    .dropDuplicates(["constructorId"])\
    .join(constructorMap, "constructorId")\
    .select("totalChampWins", "totalRaceWins", "name")\
    .orderBy(F.col("totalChampWins").desc(), F.col("totalRaceWins").desc())\




