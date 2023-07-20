
from pyspark.sql import SparkSession    


spark = SparkSession.builder.appName('reduce bu key').master('local[*]').getOrCreate()

sc = spark.sparkContext

tech = ['hadoop','hadoop','hadoop','pyspark','pyspark','pyspark','pyspark','python','python']

myrdd = sc.parallelize(tech)

mypairrdd = myrdd.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)

for x in mypairrdd.collect():
    print(x)
    

