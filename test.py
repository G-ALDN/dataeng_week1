import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import pandas as pd
from math import sqrt
# TODO:
# Refactor to use DataFrames
# Save output to Parquet
# ----------------------------
# Main Spark setup
# ----------------------------
spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel('WARN')
# ----------------------------
# S3 paths (CHANGE THIS)
# For local, use ml-100k
# For server, use s3a
# ----------------------------
#MOVIES_PATH = "s3a://rev-spark-609375805055-us-east-2-an/ml-1m/movies.dat"
#RATINGS_PATH = "s3a://rev-spark-609375805055-us-east-2-an/ml-1m/ratings.dat"
MOVIES_PATH = "./ml-100k/u.item"
RATINGS_PATH = "./ml-100k/u.data"
movieNamesSchema = StructType([
    StructField("movieID", IntegerType(), True),
    StructField("movieTitle", StringType(), True)
])
ratingsSchema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])
# ----------------------------
# Load and broadcast movie names
# ----------------------------
print("Loading movie names from S3...")
names = (
    spark.read
    .option("sep", "|")
    .option("charset", "ISO-8859-1")
    .schema(movieNamesSchema)
    .csv("./ml-100k/u.item")
)
#nameDict = func.broadcast(names)
namesDict = dict(names.select("movieID", "movieTitle").rdd.map(tuple).collect())
nameDict = spark.sparkContext.broadcast(namesDict)
# ----------------------------
# Load ratings from S3
# ----------------------------
print("Loading ratings from S3...")
ratings = (
    spark.read
    .option("sep", "\t")
    .schema(ratingsSchema)
    .csv(RATINGS_PATH)
)
# ----------------------------
# Build movie pairs
# ----------------------------
#ratingsPartitioned = ratings.partitionBy(100)
ratingsPartitioned = ratings.repartition(100)
# Assign aliases to distinguish between components of self join
ratingsA = ratingsPartitioned.alias("a")
ratingsB = ratingsPartitioned.alias("b")
#joinedRatings = ratingsPartitioned.join(ratingsPartitioned)
joinedRatings = ratingsA.join(ratingsB, 'userID')
# Select only the columns we care about
moviesAndRatings = joinedRatings.select(
    func.col("a.movieID").alias("movieIDA"),
    func.col("b.movieID").alias("movieIDB"),
    func.col("a.rating").alias("ratingA"),
    func.col("b.rating").alias("ratingB")
)
moviesAndRatings.printSchema()
#uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)
#moviePairs = uniqueJoinedRatings.map(makePairs).partitionBy(100)
#moviePairs = moviesAndRatings.where(func.col("movieIDA") < func.col("movieIDB")).repartition(100)
moviePairs = moviesAndRatings.where(func.col("movieIDA") < func.col("movieIDB")).repartition(10)
#moviePairRatings = moviePairs.groupByKey()
#moviePairRatings = moviePairs.groupBy(func.col("movieIDA"), func.col("movieIDB"))
moviePairRatingsSquares = (
    moviePairs.groupBy(func.col("movieIDA"), func.col("movieIDB"))
    .agg(
        func.sum(func.col("ratingA") * func.col("ratingA")).alias("sumAA"),
        func.sum(func.col("ratingB") * func.col("ratingB")).alias("sumBB"),
        func.sum(func.col("ratingA") * func.col("ratingB")).alias("sumAB"),
        func.count(func.col("ratingA")).alias("numPairs")
    )
)
moviePairRatingsSquares.printSchema()
moviePairRatingsWithDenom = moviePairRatingsSquares.withColumn(
    "denominator", func.sqrt(func.col("sumAA") * func.col("sumBB"))
)
moviePairRatings = moviePairRatingsWithDenom.withColumn(
    "score", func.when(func.col("denominator") < 0.0, 0.0).otherwise(func.col("sumAB") / func.col("denominator"))
)
moviePairSimilarities = moviePairRatings.select(
    func.col("movieIDA"),
    func.col("movieIDB"),
    func.col("score"),
    func.col("numPairs")
).persist()
moviePairSimilarities.printSchema()
# ----------------------------
# Compute similarities
# ----------------------------
# Optional: save full results
#moviePairSimilarities.write.parquet("s3a://rev-spark-609375805055-us-east-2-an/output/movie-sims")
#moviePairSimilarities.write.parquet("./movie-sims.parquet")
# ----------------------------
# Query similar movies
# ----------------------------
movieID = 50
scoreThreshold = 0.97
coOccurrenceThreshold = 50
#filteredResults = moviePairSimilarities.filter(lambda pairSim: (pairSim[0][0] == movieID or pairSim[0][1] == movieID) and pairSim[1][0] > scoreThreshold and pairSim[1][1] > coOccurrenceThreshold)
filteredResults = moviePairSimilarities.where(
    (func.col("movieIDA") == movieID)
    | (func.col("movieIDB") == movieID)
    & (func.col("score") > scoreThreshold)
    & (func.col("numPairs") > coOccurrenceThreshold)
)
#results = filteredResults.map(lambda pairSim: (pairSim[1], pairSim[0])).sortByKey(ascending=False).take(10)
results = filteredResults.orderBy(func.col("score"), func.col("numPairs"), ascending=True).take(10)
print("Filtering done")
print("\nTop 10 similar movies for:",
        nameDict.value[movieID])
for movieIDA, movieIDB, score, strength in results:
    similarMovieID = movieIDA if movieIDA != movieID else movieIDB
    print(
        nameDict.value[similarMovieID],
        "\tscore:", score,
        "\tstrength:", strength
    )
spark.stop()