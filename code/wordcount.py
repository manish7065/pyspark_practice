#from pyspark import SparkContext
from pyspark.sql import SparkSession

sc=SparkContext=SparkSession.builder.appName("word count").master("local[*]").getOrCreate().sparkContext

list = ["hadoop", "hadoop", "pyspark", "pyspark", "pyspark", "python", "python", "pyspark", "pyspark"]

mywordsrdd = sc.parallelize(list)
mywordspair = mywordsrdd.map( lambda word : (word,1))
mywordscount = mywordspair.reduceByKey( lambda x,y:x+y )

for word in mywordscount.collect():
	print(word)
