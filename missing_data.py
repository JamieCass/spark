from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('miss').getOrCreate()

df = spark.read.csv('/Users/jamie/Coding/Python-and-Spark-for-Big-Data-master/Spark_DataFrames/ContainsNull.csv',
                    header = True, inferSchema = True)

df.show()

# ------------------------------ DROPPING DATA ------------------------------

#to drop any row that has any missing data we use the .na.drop()
df.na.drop().show()

# add a thresh argument in (this will only drop a row that has x numbers of data missing)
df.na.drop(thresh = 2).show() # this will drop any rows that have 2 or more bits of info missing

# you can also use the how method
df.na.drop(how='all').show() # this will still come up with all the data because every row still have an Id

# to drop any that have missing data in a certain column we use subset
df.na.drop(subset=['Sales']).show() # this drops any rows that have a null in the sales column


# ------------------------------ FILLING DATA ------------------------------
# i will get to this when i have some spare time
