import sys
from pyspark import SparkConf, SparkContext
from math import sqrt


# ----------------------------
# Compute cosine similarity
# ----------------------------
def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0

    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    denominator = sqrt(sum_xx) * sqrt(sum_yy)
    score = (sum_xy / denominator) if denominator else 0.0

    return (score, numPairs)


# ----------------------------
# Filter duplicate movie pairs
# ----------------------------
def filterDuplicates(userRatings):
    ratings = userRatings[1]
    movie1 = ratings[0][0]
    movie2 = ratings[1][0]
    return movie1 < movie2


# ----------------------------
# Make ((movie1, movie2), (r1, r2))
# ----------------------------
def makePairs(userRatings):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return ((movie1, movie2), (rating1, rating2))


# ----------------------------
# Load movie names from S3
# ----------------------------
def loadMovieNames(sc, path):
    lines = sc.textFile(path)

    return lines \
        .map(lambda line: line.split("::")) \
        .map(lambda x: (int(x[0]), x[1])) \
        .collectAsMap()


# ----------------------------
# Main Spark setup
# ----------------------------
conf = SparkConf()
sc = SparkContext(conf=conf)

sc.setLogLevel("WARN")

# ----------------------------
# S3 paths (CHANGE THIS)
# ----------------------------
MOVIES_PATH = "s3a://spark-rev-571600835123-us-east-2-an/ml-1m/movies.dat"
RATINGS_PATH = "s3a://spark-rev-571600835123-us-east-2-an/ml-1m/ratings.dat"


# ----------------------------
# Load and broadcast movie names
# ----------------------------
print("Loading movie names from S3...")
nameDict = sc.broadcast(loadMovieNames(sc, MOVIES_PATH))


# ----------------------------
# Load ratings from S3
# ----------------------------
print("Loading ratings from S3...")
data = sc.textFile(RATINGS_PATH)


ratings = data \
    .map(lambda l: l.split("::")) \
    .map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))


# ----------------------------
# Build movie pairs
# ----------------------------
ratingsPartitioned = ratings.partitionBy(100)

joinedRatings = ratingsPartitioned.join(ratingsPartitioned)

uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

moviePairs = uniqueJoinedRatings.map(makePairs).partitionBy(100)

moviePairRatings = moviePairs.groupByKey()


# ----------------------------
# Compute similarities
# ----------------------------
moviePairSimilarities = moviePairRatings \
    .mapValues(computeCosineSimilarity) \
    .persist()


# Optional: save full results


res_df = moviePairSimilarities.toDF([])
moviePairSimilarities.saveAsTextFile("s3a://spark-rev-571600835123-us-east-2-an/output/movie-sims")


# ----------------------------
# Query similar movies
# ----------------------------
if len(sys.argv) > 1:

    movieID = int(sys.argv[1])

    scoreThreshold = 0.97
    coOccurrenceThreshold = 50

    filteredResults = moviePairSimilarities.filter(
        lambda pairSim:
            (pairSim[0][0] == movieID or pairSim[0][1] == movieID)
            and pairSim[1][0] > scoreThreshold
            and pairSim[1][1] > coOccurrenceThreshold
    )

    results = filteredResults \
        .map(lambda pairSim: (pairSim[1], pairSim[0])) \
        .sortByKey(ascending=False) \
        .take(10)

    print("\nTop 10 similar movies for:",
          nameDict.value[movieID])

    for sim, pair in results:
        similarMovieID = pair[0] if pair[0] != movieID else pair[1]

        print(
            nameDict.value[similarMovieID],
            "\tscore:", sim[0],
            "\tstrength:", sim[1]
        )