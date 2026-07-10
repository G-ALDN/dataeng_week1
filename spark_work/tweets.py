import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, udtf
from pyspark.sql.types import IntegerType, StringType

spark = SparkSession.builder.appName('Hashtag Extractor').getOrCreate()

data = [("Learning #AI with #ML",),("Explore #DataScient",),("No Hashtags",)]

df = spark.createDataFrame(data, ['text'])

spark.sparkContext.setLogLevel("WARN")


@udf(returnType=IntegerType())
def count_hashtags(text: str):
	if text:
		return len(re.findall(r"#\w+", text))

@udtf(returnType="hashtag: string")
class HashtagExtractor:
	def eval(self, text:str):
		if text:
			hashtags = re.findall(r"#\w+", text)
			for hashtag in hashtags:
				yield (hashtag,)
	
spark.udf.register('count_hashtags', count_hashtags)
spark.udtf.register('hash_ex', HashtagExtractor)

spark.sql("SELECT count_hashtags('Welcome to #ApacheSpark and #BigData') AS hashtag_count").show()

df.selectExpr('text', 'count_hashtags(text) AS num_hashtags').show()

df.createOrReplaceTempView("tweets")

spark.sql("""
SELECT text, hashtag FROM tweets, LATERAL hash_ex(text)
""").show()

spark.stop()