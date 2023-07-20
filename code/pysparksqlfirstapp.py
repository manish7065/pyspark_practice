from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegetType,StructField 
spark = SparkSession.builder.appName('first app').master('Local[*]').geOrCreate()

df = spark.read.json("cars.json")
df.printschema()

#defining manual schema
schema=StructType([
StructField("age",IntegerType(), True),
structField("name",StringType(), True)
])

df2withschema = spark.read.json("people.json").schema(schema)


df.show()

