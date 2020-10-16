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
# to fill everything with a string (sspark will fill all fields that have values as a string )
df.na.fill('FILL VALUE').show()

# to fill in everything with a numeric value (spark will again fill in any fields that have a numeric value)
df.na.fill(0).show()


# to spicfy what fields/colums you want to fill (declare the subset)
df.na.fill('No Name', subset = ['Name']).show()

#-------------- common practice --------------
# fill all numerice values with the mean value
# first import the function
from pyspark.sql.functions import mean

mean_val = df.select(mean(df['Sales'])).collect()
mean_sales = mean_val[0][0] # remember we need to index because it will return a list
df.na.fill(mean_sales, ['Sales']).show()

# to put it all on 1 line
df.na.fill(df.select(mean(df['Sales'])).collect()[0][0], ['Sales']).show()
