from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window

spark = SparkSession.builder\
      .config("spark.sql.shuffle.partitions", 4)\
      .master("local[4]")\
      .getOrCreate()\


m = defaultdict(lambda: "na", {a : "b", 
            b : "c"})\

milista = ["a",
    "b"]

lastRace = Window.partitionBy("year")\

lastRaces = spark.read.format("csv")\
    .option("header", "True")\
    .option("sep", ",")\
    .load("../data/races.csv")\
    .where((F.col("year") >= 1990) & (F.col("year") <= 1999) | (F.col("year") <= 1998))\
    .withColumn("round", F.col("round").cast(T.IntegerType()))\
    .withColumn("max", F.max(F.col("round")).over(lastRace))\
    .withColumn("test", F.when((F.col("round") == F.col("max")) | (F.col("round") == 1), 23))\
    .select("raceId", "year")\

constructors = Window.partitionBy("constructorId")\

constructorWinners = spark.read.format("csv")\
    .option("header", "True")\
    .option("sep", ",")\
    .load("../data/constructor_standings.csv")\
    .join(lastRaces, ["raceId"], "right")\
    .where(F.col("position") == 1)\
    .select("constructorId", "wins", "year")\
    .withColumn("totalChampWins", F.count(F.col("constructorId")).over(constructors))\
    .withColumn("totalRaceWins", F.sum(F.col("wins")).over(constructors).cast(T.IntegerType()))\
    .drop("wins")\

constructors = spark.read.format("csv")\
    .option("header", "True")\
    .option("sep", ",")\
    .load("../data/constructors.csv")\
    .select("constructorId", "name")\

mostDominantConstructor = constructorWinners\
    .drop("year")\
    .dropDuplicates("constructorId")\
    .orderBy(F.col("totalChampWins").desc(), F.col("totalRaceWins").desc())\
    .join(constructors, "constructorId")\
    .drop("constructorId")\



