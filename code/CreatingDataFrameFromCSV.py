from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Demo").master("local[*]").getOrCreate()

df =spark.read.csv("sample.txt")
df.show()