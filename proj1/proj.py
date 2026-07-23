# Import the SparkSession class, which is the entry point for working with Spark.
from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import BooleanType, StructField, StringType, StructType, IntegerType, FloatType, DateType


# Create and configure a Spark session.
spark = (
    SparkSession.builder

    # Set a name for the Spark application (shows up in Spark UI/logs).
    .appName("CustomerOrders")

    # Enable Apache Iceberg SQL extensions so Spark understands
    # Iceberg-specific SQL commands and table operations.
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    # Register a catalog named "glue_catalog".
    # Spark will use this catalog whenever tables are referenced with
    # the prefix "glue_catalog".
    .config(
        "spark.sql.catalog.glue_catalog",
        "org.apache.iceberg.spark.SparkCatalog"
    )

    # Tell Spark that this catalog should use AWS Glue
    # as the metadata store for Iceberg tables.
    .config(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog"
    )

    # Specify the S3 warehouse location where Iceberg table data
    # and metadata files will be stored.
    .config(
        "spark.sql.catalog.glue_catalog.warehouse",
        "s3://spark-rev-571600835123-us-east-2-an/iceberg/"
    )

    # Configure Iceberg to use the S3FileIO implementation
    # for reading and writing data in Amazon S3.
    .config(
        "spark.sql.catalog.glue_catalog.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO"
    )

    # Create the Spark session with all of the above settings.
    .getOrCreate()
)


orders_schema = StructType ([
    StructField('order_id', IntegerType()),
    StructField('customer_id', IntegerType()),
    StructField('product_id', IntegerType()),
    StructField('order_date', DateType()),
    StructField('ship_date', DateType()),
    StructField('quantity', IntegerType()),
    StructField('unit_price', FloatType()),
    StructField('discount_pct', FloatType()),
    StructField('total_amount', FloatType()),
    StructField('payment_method', StringType()),
    StructField('order_status', StringType())
])

products_schema = StructType ([
    StructField('product_id', IntegerType()),
    StructField('product_name', StringType()),
    StructField('category', StringType()),
    StructField('brand', StringType()),
    StructField('price', FloatType()),
    StructField('cost', FloatType()),
    StructField('stock_quantity', IntegerType()),
    StructField('weight_kg', FloatType()),
    StructField('created_date', DateType()),
    StructField('is_active', BooleanType())
])

customers_schema = StructType ([
    StructField('customer_id', IntegerType()),
    StructField('first_name', StringType()),
    StructField('last_name', StringType()),
    StructField('email', StringType()),
    StructField('phone', StringType()),
    StructField('signup_date', DateType()),
    StructField('country', StringType()),
    StructField('state', StringType()),
    StructField('postal_code', StringType()),
    StructField('is_active', BooleanType()),
    StructField('loyalty_points', IntegerType())
])


orders_df = spark.read.options(
	header=True,
	schema=orders_schema
).csv(
	"s3://spark-rev-571600835123-us-east-2-an/orders.csv"
)

products_df = spark.read.options(
	header=True,
	schema=products_schema
).csv(
	"s3://spark-rev-571600835123-us-east-2-an/products.csv"
)

customer_df = spark.read.options(
	header=True,
	schema=customers_schema
).csv(
	"s3://spark-rev-571600835123-us-east-2-an/customers.csv"
)



# Create an Iceberg database (namespace) in AWS Glue if it
# doesn't already exist.
spark.sql("""
CREATE DATABASE IF NOT EXISTS glue_catalog.iceberg_catalog_db
""")



### CUSTOMERS

# Regex pattern for a standard email
email_pattern = r"([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)"
phone_pattern = r"(?:\+1\s*)?\(?([0-9]{3})\)?[-.\s]*([0-9]{3})[-.\s]*([0-9]{4})"

customer_df_cleaned = (
						customer_df.withColumn('first_name', func.trim(func.col('first_name')))
						.withColumn('last_name', func.trim(func.col('last_name')))
						.withColumn('email', func.trim(func.col('email')))
						.withColumn('phone', func.trim(func.col('phone')))
						.withColumn('country', func.trim(func.col('country')))
						.withColumn('state', func.trim(func.col('state')))
						.withColumn('postal_code', func.trim(func.col('postal_code')))
						.withColumn("email", func.when(func.col("email").rlike(email_pattern), func.col("email")))
						.withColumn("loyalty_points", func.expr("try_cast(loyalty_points as int)"))
)

phone_pattern = r"^(\+?\d{1,3}[\s.-]?)?(\(\d{3}\)|\d{3})[\s.-]?\d{3}[\s.-]?\d{4}$"

customer_df_cleaned = customer_df_cleaned.withColumn(
    "phone",
    func.when(
        func.col("phone").rlike(phone_pattern),
        func.col("phone")
    )
)

customer_df_cleaned = customer_df_cleaned.dropna().drop_duplicates()

(
    customer_df_cleaned.writeTo(
        # Fully qualified table name:
        # catalog.database.table
        "glue_catalog.iceberg_catalog_db.customers"
    )

    # Specify that the table format should be Apache Iceberg.
    .using("iceberg")

    # Create the table if it doesn't exist.
    # If it already exists, replace it with the new data.
    .createOrReplace()
)

customer_df_cleaned.show()


# Products
products_df_cleaned = (
						products_df
						.withColumn(
                            "price",
                            func.regexp_replace(func.col("price"), r"[\$,]", "").try_cast("float"))
						.withColumn(
                            "price",
                            func.when(func.col("price") < 0, func.lit(0.0)).otherwise(func.col("price")))
						.withColumn(
                            "price",
                            func.round(func.col('price'), func.lit(2)))
						.withColumn(
                            "cost",
                            func.round(func.col('cost').try_cast('float'), func.lit(2)))
						.withColumn(
                            "stock_quantity",
                            func.col('stock_quantity').try_cast('int'))
						.withColumn(
                            "stock_quantity",
                            func.when(func.col("stock_quantity") < 0, func.lit(0))
							.otherwise(func.col("stock_quantity")))
						.withColumn(
                            "weight_kg",
                            func.round(func.col('weight_kg').try_cast('float'), func.lit(2)))
                        .withColumn(
                            "is_active",
                            func.when(func.lower(func.trim(func.col("is_active").cast("string"))).rlike("^(true|yes|y|1)$"),func.lit(True))
                            .when(func.lower(func.trim(func.col("is_active").cast("string"))).rlike("^(false|no|n|0)$"),func.lit(False))
                            .otherwise(func.lit(None)))
						.withColumn(
							"created_date",
						    func.regexp_replace(func.col("created_date"), r"[\/,]", "-"))
						.withColumn(
							"created_date",
							func.col('created_date').try_cast('date')
                        )
						.dropDuplicates()
						.dropna()
)

(
    products_df_cleaned.writeTo(
        # Fully qualified table name:
        # catalog.database.table
        "glue_catalog.iceberg_catalog_db.products"
    )

    # Specify that the table format should be Apache Iceberg.
    .using("iceberg")

    # Create the table if it doesn't exist.
    # If it already exists, replace it with the new data.
    .createOrReplace()
)

products_df_cleaned.show()


# Orders
orders_df_cleaned = (
            orders_df
            .withColumn("order_id", func.expr("try_cast(order_id as int)"))
            .withColumn("customer_id", func.expr("try_cast(customer_id as int)"))
            .withColumn("product_id", func.trim(func.col("product_id")))

            .withColumn(
				"order_date",
				func.regexp_replace(func.col("order_date"), r"[\/,]", "-"))
			.withColumn(
				"order_date",
				func.col('order_date').try_cast('date'))
			.withColumn(
				"ship_date",
				func.regexp_replace(func.col("ship_date"), r"[\/,]", "-"))
			.withColumn(
				"ship_date",
				func.col('ship_date').try_cast('date'))
            .withColumn("quantity", func.expr("try_cast(quantity as int)"))
            .withColumn(
                "quantity",
                func.when((func.col("quantity") < 0) | func.col("quantity").isNull(), func.lit(0))
                .otherwise(func.col("quantity")))
            .withColumn("unit_price", func.regexp_replace(func.col("unit_price"), r"[\$,]", ""))
            .withColumn("unit_price", func.expr("try_cast(unit_price as float)"))
            .withColumn(
                "unit_price",
                func.when((func.col("unit_price") < 0) | func.col("unit_price").isNull(), func.lit(0.0))
                .otherwise(func.round(func.col("unit_price"), 2)))
			.withColumn(
				"unit_price",
				func.round(func.col("unit_price"), 2)
            )
            .withColumn("discount_pct", func.expr("try_cast(discount_pct as float)"))
            .withColumn(
                "discount_pct",
                func.when((func.col("discount_pct") < 0) | func.col("discount_pct").isNull(), func.lit(0.0))
                .when(func.col("discount_pct") > 100, func.lit(100.0))
                .otherwise(func.round(func.col("discount_pct"), 2)))
            .withColumn(
                "total_amount",
                func.round(
                    func.col("quantity") * func.col("unit_price") * (1 - (func.col("discount_pct") / 100.0)), 2))
            .withColumn("payment_method", func.trim(func.col("payment_method")))
            .withColumn("order_status", func.trim(func.col("order_status")))
            .dropDuplicates()
            .dropna()
)

(
    orders_df_cleaned.writeTo(
        # Fully qualified table name:
        # catalog.database.table
        "glue_catalog.iceberg_catalog_db.orders"
    )

    # Specify that the table format should be Apache Iceberg.
    .using("iceberg")

    # Create the table if it doesn't exist.
    # If it already exists, replace it with the new data.
    .createOrReplace()
)

orders_df_cleaned.show()
# Stop the Spark session and release cluster resources.
spark.stop()