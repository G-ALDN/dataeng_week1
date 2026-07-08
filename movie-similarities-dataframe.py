from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.functions import udf
import sys

def computeCosineSimilarity(spark, data):
    # Compute xx, xy and yy columns
    pairScores = data \
      .withColumn("xx", func.col("rating1") * func.col("rating1")) \
      .withColumn("yy", func.col("rating2") * func.col("rating2")) \
      .withColumn("xy", func.col("rating1") * func.col("rating2")) 

    # Compute numerator, denominator and numPairs columns
    calculateSimilarity = pairScores \
      .groupBy("movie1", "movie2") \
      .agg( \
        func.sum(func.col("xy")).alias("numerator"), \
        (func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))).alias("denominator"), \
        func.count(func.col("xy")).alias("numPairs")
      )

    # Calculate score and select only needed columns (movie1, movie2, score, numPairs)
    result = calculateSimilarity \
      .withColumn("score", \
        func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator")) \
          .otherwise(0) \
      ).select("movie1", "movie2", "score", "numPairs")

    return result

spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

movieSchema = StructType([ \
                               StructField("movieId", IntegerType(), True), \
                               StructField("movieTitle", StringType(), True), \
                               StructField("genres", StringType())
                               ])
    
ratingSchema = StructType([ \
                     StructField("userId", IntegerType(), True), \
                     StructField("movieId", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])
    
    
# Create a broadcast dataset of movieID and movieTitle.
# Apply ISO-885901 charset


# Load up movie data as dataset
movies = spark.read \
      .option("sep", "::") \
      .schema(movieSchema) \
      .csv("s3://spark-rev-571600835123-us-east-2-an/ml-1m/movies.dat")

print("Loading ratings from S3...")
ratings = spark.read \
      .option("sep", "::") \
      .schema(ratingSchema) \
      .csv("s3://spark-rev-571600835123-us-east-2-an/ml-1m/ratings.dat")


ratings = ratings.select("userId", "movieId", "rating")

print("Loading movie names from S3...")
nameDict = spark.sparkContext.broadcast({
    row["movieId"]: row["movieTitle"] for row in movies.collect()
})



# Emit every movie rated together by the same user.
# Self-join to find every combination.
# Select movie pairs and rating pairs
moviePairs = ratings.alias("ratings1") \
      .join(ratings.alias("ratings2"), (func.col("ratings1.userId") == func.col("ratings2.userId")) \
            & (func.col("ratings1.movieId") < func.col("ratings2.movieId"))) \
      .select(func.col("ratings1.movieId").alias("movie1"), \
        func.col("ratings2.movieId").alias("movie2"), \
        func.col("ratings1.rating").alias("rating1"), \
        func.col("ratings2.rating").alias("rating2"))


moviePairSimilarities = computeCosineSimilarity(spark, moviePairs).cache()

output_path = "s3://spark-rev-571600835123-us-east-2-an/ml-output"

print("Saving Results to S3")
moviePairSimilarities.write.mode("overwrite").parquet(output_path)

if (len(sys.argv) > 1):
    scoreThreshold = 0.97
    coOccurrenceThreshold = 1500.0

    # the 0 index always contains the script name, so the second one '1', has our first cli param
    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter( \
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) & \
          (func.col("score") > scoreThreshold) & (func.col("numPairs") > coOccurrenceThreshold))

    # Sort by quality score.
    results = filteredResults.sort(func.col("score").desc()).take(10)
    
    print ("Top 10 similar movies for " + nameDict.value[movieID])
    
    for result in results:
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = result.movie1
        if (similarMovieID == movieID):
          similarMovieID = result.movie2
        
        print(nameDict.value[similarMovieID] + "\tscore: " \
              + str(result.score) + "\tstrength: " + str(result.numPairs))
        
