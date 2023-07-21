val words = spark.sparkContext.parallelize(Seq("manish","is","a","good","person"))
val wordPair = words.map(w=>(w.charAt(0),w))
wordPair.foreach(println)

# To create a rdd fron a file

val dataRDD = spark.read.csv("/config/workspace/data/sample.txt").rdd