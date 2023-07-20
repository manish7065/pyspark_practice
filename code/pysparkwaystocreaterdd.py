from pyspark.sql import SparkSession
# Multiple ways to crete rdd
spark = sparksession.builder.appname("ways to create rdd").master('local[*]').getorcreate()

# 1 rdd from collection object
mylist = [1,2,3,4,5,6,7,8,9]

rdd=spark.sparkcontext.parallelize(mylist)

#2 by applying T/F on existing  rdd
mynewrdd = rdd.map(lambda a: a+1)

# 3 rdd from a text file

#mytextrdd = spark.sparkcontext.textfile("sample_text.txt")


rdd.collect()
mynewrdd.collect()
#mytextrdd.collect()

