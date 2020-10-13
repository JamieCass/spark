##################################################
# Spark Basics
##################################################
# start with the import
from pyspark.sql import SparkSession

# create a session with spark
spark = SparkSession.builder.appName('Basics').getOrCreate()

# read in a file with the information. (doesnt really like file names with spaces in)
df = spark.read.json('/Users/jamie/Coding/Python-and-Spark-for-Big-Data-master/Spark_DataFrames/people.json')

# we need to call the method show in order for the data to be shown
df.show()

# to print out the schema of the df
df.printSchema()

# to print out all the column names
df.columns

# to show the statistical data for the df
df.describe().show()

# import some built in functions to be able to make changes to the schema
from pyspark.sql.types import StructField, StringType,IntegerType,StructType

# to change the age type from long to integer
data_schema = [StructField('age',IntegerType(),True),
               StructField('name',StringType(),True)]

# make the final StructType in the schema we just made
final_struct = StructType(fields = data_schema)

# read in the file again using the final_struct we created.
df = spark.read.json('/Users/jamie/Coding/Python-and-Spark-for-Big-Data-master/Spark_DataFrames/people.json',schema=final_struct)

# now age will be an integer not 'long'
df.printSchema()

# print out just the age column (we have to use select otherwise it wont give us a dataframe)
df.select('age').show()

# to ge the first 2 rows(doesnt come up the same was pandas would)
df.head(2)

# select multiple columns
df.select(['age', 'name']).show()

# to add a new column to the dataframe (wont save to the dataframe, would need to save it as a new variable)
df.withColumn('double_age', df['age']*2).show()

# rename a column
df.withColumnRenamed('age', 'my_new_age').show()

# register as a sql view
df.createOrReplaceTempView('people')

# now we can access the data with sql requests
results = spark.sql('SELECT * FROM people WHERE age < 30')
results.show()
