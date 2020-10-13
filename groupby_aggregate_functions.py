from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('aggs').getOrCreate()

df = spark.read.csv('/Users/jamie/Coding/Python-and-Spark-for-Big-Data-master/Spark_DataFrames/sales_info.csv',
                inferSchema = True, header = True)

df.show()

# groupby company (then you can use mean to get the average, min, max, sum, count(to see how many rows each compnay has in the df))
df.groupBy('Company').mean().show() # using mean to get the average.

# another way to do that is agg
df.agg({'Sales':'sum'}).show()

# this is a bit of a nicer way to gather the data
group_data = df.groupBy('Company')
group_data.agg({'Sales':'max'}).show()

# import functions from the pyspark library
from pyspark.sql.functions import countDistinct,avg,stddev

# count the number of distinct sales
df.select(countDistinct('Sales')).show()

# show the average for all sales you can then change the name using '.alias()' to whatever makes sense to you
df.select(avg('Sales').alias('Average Sales')).show()

# select standard deviation
df.select(stddev('Sales')).show()

#import format number function to make the number a bit neater
from pyspark.sql.functions import format_number

sales_std = df.select(stddev('Sales').alias('std'))
sales_std.show()

# change the number so it only shows 2 decimal places, and then also change the name to formatted std
sales_std.select(format_number('std',2).alias('Formatted std')).show()

#to sort the order of the df
df.orderBy('Sales').show()

# to have the results showing descending
df.orderBy(df['Sales'].desc()).show()
