import re
from pyspark.sql import SparkSession, functions as func
from pyspark.sql.functions import udf, udtf
from pyspark.sql.types import IntegerType, StringType, StructField, StructType



spark = SparkSession.builder.appName("Access Log").getOrCreate()

df = spark.read.text("access_log.txt", lineSep="\n")

df2 = df.withColumn("parts", func.split("value", " "))
df3 = (
    df2.withColumn("field0", func.col("parts")[0])
       .withColumn("field1", func.col("parts")[1])
       .withColumn("field2", func.col("parts")[2])
        .withColumn("field3", func.col("parts")[3])
       .withColumn("field4", func.col("parts")[4])
       .withColumn("field5", func.col("parts")[5])
       .withColumn("field6", func.col("parts")[6])
        .withColumn("field7", func.col("parts")[7])
       .withColumn("field8", func.col("parts")[8])
       .withColumn("field9", func.col("parts")[9])
)
df3 = df3.drop("value")
df3 = df3.drop("parts")


reqs = df3.groupBy('field0').count().sort('count', ascending=False)
reqs.show()

endps = df3.groupBy('field6').count().sort('count', ascending=False)
endps.show()

status = df3.groupBy('field8').count().sort('count', ascending=False)
status.show()

spark.stop()


