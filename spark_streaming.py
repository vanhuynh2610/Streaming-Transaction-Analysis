from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, to_date, year, month 
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import from_json
from pyspark.sql.functions import to_json, struct
import pyspark
from pyspark.sql.functions import date_trunc


spark = SparkSession.builder \
    .appName('PySpark Ecommerce Streaming') \
    .master("local[*]") \
    .config('spark.jars','postgresql-42.7.3.jar') \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

schema = StructType([
    StructField("transactionId", StringType()),
    StructField("productId", StringType()),
    StructField("productName", StringType()),
    StructField("productBrand", StringType()),
    StructField("productPrice", DoubleType()),
    StructField("productQuantity", IntegerType()),
    StructField("totalPrice", DoubleType()),
    StructField("customerId", StringType()),
    StructField("transactionDate", TimestampType()),
    StructField("paymentMethod", StringType())
])

kafka_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'broker:29092') \
    .option('subscribe', 'financial_transactions') \
    .option('startingOffsets', 'latest') \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

def stream_writing(transactions_df, table_name):
    def spark_writing(df , batch_id):
        print("Batch id" + str(batch_id))
        (
            df.write
            .mode("append")
            .format("jdbc")
            .option("driver", "org.postgresql.Driver")
            .option("url", "jdbc:postgresql://host.docker.internal:6432/postgres")
            .option("dbtable", table_name)
            .option("user", "postgres1")
            .option("password", "postgres12")
            .save()
        )

    query = (transactions_df
    .writeStream 
    .foreachBatch(spark_writing)
    .trigger(processingTime='10 seconds')
    .option("checkpointLocation", f"checkpoint/{table_name}v4")
    .outputMode("append")
    .start() )

    return query


transactions_df = kafka_df.withColumn("transactionDate", col("transactionDate").cast(TimestampType()))
transactions_data = transactions_df.withWatermark("transactionDate", "1 minute")
sales_per_category_df = (transactions_data
    .groupBy(col("productBrand")) 
    .agg(sum("totalPrice").alias("total_sales")))

sales_per_day_df = transactions_data \
    .groupBy(to_date(col("transactionDate")).alias("transaction_date")) \
    .agg(sum("totalPrice").alias("total_sales"))

sales_per_payment_df = transactions_data \
    .groupBy(col("paymentMethod").alias("payment_method")) \
    .agg(sum("totalPrice").alias("total_sales"))

sales_per_hour_df = transactions_data \
    .groupBy(date_trunc("hour", col("transactionDate")).alias("transaction_hour")) \
    .agg(sum("totalPrice").alias("total_sales"))

# sales_per_month_df = transactions_df \
#     .groupBy(year(col("transactionDate")).alias("year"), month(col("transactionDate")).alias("month")) \
#     .agg(sum("totalPrice").alias("total_sales"))


query1 = stream_writing(kafka_df, "transactions12")


query2 = sales_per_category_df.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("topic", "sales_per_category") \
        .option("checkpointLocation", "checkpoint/checkpoint_category") \
        .outputMode("update") \
        .start()


query3 = sales_per_day_df.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("topic", "sales_per_day") \
        .option("checkpointLocation", "checkpoint/checkpoint_day") \
        .outputMode("update") \
        .start()

query4 = sales_per_hour_df.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("topic", "sales_per_hour") \
        .option("checkpointLocation", "checkpoint/checkpoint_hour") \
        .outputMode("update") \
        .start()


query5 = sales_per_payment_df.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("topic", "sales_per_payment") \
        .option("checkpointLocation", "checkpoint/checkpoint_payment") \
        .outputMode("update") \
        .trigger(processingTime='10 seconds') \
        .start()


query5.awaitTermination()
