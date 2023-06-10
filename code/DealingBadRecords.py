from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("d").master("local").getOrCreate()

    print("------PERMISSIVE MODE.....")
    df = spark.read.option("columnNameOfCorruptRecord","Rejected Records").json(path="../dataset/access_logs.json")
    df.show(truncate=False)
    print("------DROPMALFORMED MODE.....")
    df = spark.read.json(path="../dataset/access_logs.json", mode="DROPMALFORMED")
    df.show(truncate=False)
    print("------FAILFAST MODE.....")
    df = spark.read.json(path="../dataset/access_logs.json", mode="FAILFAST")
    df.show(truncate=False)
