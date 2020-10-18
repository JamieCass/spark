from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('dates').getOrCreate()

df = spark.read.csv('/Users/jamie/Coding/Python-and-Spark-for-Big-Data-master/Spark_DataFrames/appl_stock.csv',
                    header = True, inferSchema = True)

df.show()

# import some functions that will be useful
from pyspark.sql.functions import (dayofmonth, hour,
                                dayofyear, month,
                                year, weekofyear,
                                format_number, date_format)
# to show just the month from the date column
df.select(dayofmonth(df['Date'])).show()

# to show the hour (they will all be 0 because there isnt any real data there for the time)
df.select(hour(df['Date'])).show()

# to show the month (you can see the pattern here)
df.select(month(df['Date'])).show()

#-------------- more realistic --------------
# create a new df with a column with the year in
newdf = df.withColumn('Year', year(df['Date']))

# show the average closing price for each year
result = newdf.groupBy('Year').mean().select(['Year', 'avg(Close)'])
result.show()
# change the name of the acg(Close)
new = result.withColumnRenamed('avg(Close)', 'Average Closing Price')
# change the result to have only 2 decimal points
new.select(['Year', format_number('Average Closing Price', 2).alias('Avg Close')]).show() # we had to change the name again by using the .alias
