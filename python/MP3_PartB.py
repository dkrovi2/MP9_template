from pyspark import SparkContext, SQLContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType

sc = SparkContext()
sqlContext = SQLContext(sc)

####
# 1. Setup (10 points): Download the gbook file and write a function to load it in an RDD & DataFrame
####

# RDD API
# Columns:
# 0: place (string), 1: count1 (int), 2: count2 (int), 3: count3 (int)


# Spark SQL - DataFrame API

####
# 2. Counting (10 points): How many lines does the file contains? Answer this question via both RDD api & #Spark SQL
####

# Spark SQL 


# +--------+                                                                              
# |count(1)|
# +--------+
# |86618505|
# +--------+


lines = sc.textFile("gbooks", 1)
parts = lines.map(lambda l: l.split("\t"))
records = parts.map(lambda p: {"word": p[0], "count1": int(p[1]), "count2": int(p[2]), "count3": int(p[3])})
schema = sqlContext.createDataFrame(records)
schema.createOrReplaceTempView("gbooks")
