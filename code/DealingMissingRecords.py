from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("d").master("local").getOrCreate()
    df = spark.read.csv(path="../dataset/employee-missing-records.dat",
                        header=True,
                        inferSchema=True)

    df.printSchema()
    df.show()
    df.na.drop().show()

    df.na.drop(subset=["col_name"]).show()

    df.na.fill("NULL IN SOURCE").na.fill(0).na.replace(-1, 1, subset=["col_salary"]).na.replace(-2, 2, subset=[
        "col_salary"]).show()
