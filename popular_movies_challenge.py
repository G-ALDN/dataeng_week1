import re
from pyspark.sql import SparkSession, functions as func
from pyspark.sql.functions import udf, udtf
from pyspark.sql.types import IntegerType, StringType, StructField, StructType



spark = SparkSession.builder.appName("Popular Movies").getOrCreate()

schema = StructType([
	StructField('userid', IntegerType()),
	StructField('movieid', IntegerType()),
	StructField('rating', IntegerType()),
	StructField('timestamp', StringType())
])

df = spark.read.option("sep", "\t").schema(schema).csv("./ml-100k/u.data")

df.groupBy("movieid").agg(func.count('rating').alias('num_ratings'), func.sum('rating').alias('sum_ratings')).sort('sum_ratings', ascending=False).show(10)



spark.stop()