from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Ways to Create RDD").master('local[*]').getOrCreate()

# 1. RDD from a collection object
my_list = [1, 2, 3, 4, 5, 6, 7, 8, 9]

rdd = spark.sparkContext.parallelize(my_list)

# 2. Applying transformation on an existing RDD
my_new_rdd = rdd.map(lambda a: a + 1)

# 3. RDD from a text file
text_rdd = spark.sparkContext.textFile("sample.txt")

# Collect and print the RDD data
rdd.collect()
my_new_rdd.collect()
text_rdd.collect()


