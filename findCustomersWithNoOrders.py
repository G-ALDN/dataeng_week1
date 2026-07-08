# expected output:

# | customer_id | first_name | last_name |
# | ----------- | ---------- | --------- |
# | 1002        | Bob        | Smith     |
# | 1004        | David      | Wilson    |
# | 1007        | Grace      | Moore     |
# | 1009        | Ivy        | Anderson  |
# | 1013        | Mia        | Martin    |
# | 1015        | Olivia     | Garcia    |

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import DateType, FloatType, StructType, StructField, StringType, IntegerType, LongType




spark = SparkSession.builder.appName("Customer No Orders").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

customer_schema = StructType([
    StructField('customerId', IntegerType()),
    StructField('firstName', StringType()),
    StructField('lastName', StringType()),
    StructField('email', StringType()),
    StructField('city', StringType())
])

orders_schema = StructType([
    StructField('orderId', IntegerType()),
    StructField('customerId', IntegerType()),
    StructField('orderDate', DateType()),
    StructField('amount', FloatType())
])

customers = spark.read.option('header', True).schema(customer_schema).csv('customers.csv')
orders = spark.read.option('header', True).schema(orders_schema).csv('orders.csv')

# customers.show()
# orders.show()

#customers_with_no_orders = customers.join(orders, on="customerId", how="left").where(func.col('orderId').isNull())
customers_with_no_orders = customers.join(orders, on="customerId", how="left_anti")


customers_with_no_orders.select('customerId', 'firstName', 'lastName').show()