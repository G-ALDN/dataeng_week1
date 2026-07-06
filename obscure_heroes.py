from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName('Superheroes').getOrCreate()

spark.sparkContext.setLogLevel('WARN')

schema = StructType([
	StructField('id', IntegerType()),
	StructField('name', StringType())
])

names = spark.read.schema(schema).option('sep', ' ').csv('MarvelNames.txt')

lines = spark.read.text('MarvelGraph.txt')

connections = lines.withColumn('id', func.split(func.col('value'), ' ')[0]) \
.withColumn('connections', func.size(func.split(func.col('value'), ' ')) - 1) \
.groupBy('id').agg(func.sum('connections').alias('connections'))

most_obscure = connections.sort(func.col('connections'))

min_connections = most_obscure.agg(func.min('connections')).first()[0]

most_obscure_name = names.join(most_obscure, 'id').select('id', 'name')

most_obscure_name.join(most_obscure, 'id').filter(func.col('connections') == min_connections).show(truncate=False)

spark.stop()