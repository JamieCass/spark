from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('ops').getOrCreate()

df = spark.read.csv('/Users/jamie/Coding/Python-and-Spark-for-Big-Data-master/Spark_DataFrames/appl_stock.csv',
                    inferSchema = True, header = True)

df.printSchema()
df.head(3)[0]

# --------------- Typical use cases (realistic) ---------------

# filter to show results where closing price < $500
df.filter('Close < 500').show()

# to show just the open and close columns
df.filter('Close < 500').select(['Open', 'Close']).show()

# filtering using more python syntax than sql (how we will be doing most of the queries)
df.filter(df['Close'] < 500).show()

# to show results for open price > 200 and close price < 200
# (we need to use '&' istead of 'and' and also make sure we put our seperate queries in parens)
df.filter( (df['Close'] < 200) & (df['Open'] > 200)).show() #use the '~' for NOT instead of using a '!' like you would usually in python

# to select a specific result (use collect() so we can save it as a variable)
result = df.filter(df['Low'] == 197.16).collect()
# show it as a dcit
row = result[0]
row.asDict()['Volume']
