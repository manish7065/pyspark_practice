from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("map transformation").master('local[*]').getOrCreate()

# Create a SparkContext
sc = spark.sparkContext

words = ["JANUARY, FEBRUARY, MARCH, APRIL, JUNE, JULY, AUGUST, SEPTEMBER, OCTOBER, NOVEMBER, DECEMBER"]

words_rdd = sc.parallelize(words)

my_new_words_rdd = words_rdd.flatMap(lambda word: (word, len(word)))

for word in my_new_words_rdd.collect():
    print(word)

