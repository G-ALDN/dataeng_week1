from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName('Superheroes').getOrCreate()

schema = StructType([
	StructField('id', IntegerType()),
	StructField('name', StringType())
])

names = spark.read.schema(schema).option('sep', ' ').csv('MarvelNames.txt')

lines = spark.read.text('MarvelGraph.txt')

connections = lines.withColumn('id', func.split(func.col('value'), ' ')[0]) \
.withColumn('connections', func.size(func.split(func.col('value'), ' ')) - 1) \
.groupBy('id').agg(func.sum('connections').alias('connections'))

most_popular = connections.sort(func.col('connections').desc()).first()

most_popular_name = names.filter(func.col('id') == most_popular[0]).select('name').first()

print(f"{most_popular_name[0]} {most_popular[1]}")

connections.show()

spark.stop()