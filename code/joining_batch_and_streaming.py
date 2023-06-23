from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Test APp").master("local").getOrCreate()

    customer_details_schema = StructType([
        StructField("customer_id", IntegerType()),
        StructField("gender", StringType()),
        StructField("age", IntegerType())
    ])

    customer_details_df = spark.read.csv(
        path="../dataset/streaming_data/customer_transaction/static_datasets/join_static_personal_details.csv",
        schema=customer_details_schema)

    customer_transaction_schema = StructType([
        StructField("customer_id", IntegerType()),
        StructField("transaction_amount", IntegerType()),
        StructField("transaction_rating", IntegerType())
    ])

    customer_transaction_df = spark.readStream.csv(
        path="../dataset/streaming_data/customer_transaction/streaming_datasets/input",
        schema=customer_transaction_schema
    )

    result_table = customer_details_df.join(customer_transaction_df,
                                            customer_details_df["customer_id"] == customer_transaction_df[
                                                "customer_id"])
    result_table.writeStream.format("console").outputMode("append").start().awaitTermination()
