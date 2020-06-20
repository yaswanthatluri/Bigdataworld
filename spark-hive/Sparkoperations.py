#infer schema to data
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import Column
from pyspark.sql.functions import expr, col
from pyspark.sql.types import StructField, StructType,StringType,IntegerType

df_schema = StructType([StructField("type_s",StringType(),True),StructField("tasks",IntegerType(),True)])

rawdf= spark.read.format("csv").schema(df_schema).load("s3://yash-east/sony_case/starting52.csv")

rawdf.printSchema()

#Example Schema
===========
schema = StructType([
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

#partitions
===========
# Write file to disk in parquet format partitioned by year - overwrite any existing file
df.write.partitionBy('year').format('parquet').mode('overwrite').save(parquet_file_path)

# Write file to disk in parquet format partitioned by year - append to an existing file
df.write.partitionBy('year').format('parquet').mode('append').save(parquet_file_path)

# Write data frame as a Hive table
df.write.bucketBy(10, "year").sortBy("avg_ratings").saveAsTable("films_bucketed")


#modifying columns
=============
# Create a column with the default value = 'xyz'
df = df.withColumn('new_column', F.lit('xyz'))

# Create a column with default value as null
df = df.withColumn('new_column', F.lit(None).cast(StringType()))

# Create a column using an existing column
df = df.withColumn('new_column', 1.4 * F.col('existing_column'))

# Another example using the MovieLens database
df = df.withColumn('test_col3', F.when(F.col('avg_ratings') < 7, 'OK')\
                                 .when(F.col('avg_ratings') < 8, 'Good')\
                                 .otherwise('Great')).show()

# Create a column using a UDF

def categorize(val):
  if val < 150: 
    return 'bucket_1'
  else:
    return 'bucket_2'
    
my_udf = F.udf(categorize, StringType())

df = df.withColumn('new_column', categorize('existing_column'))

#changing column names
============
# Changing column name with withColumnRenamed feature
df = df.withColumnRenamed('existing_column_name', 'new_column_name')

# Changing column with selectExpr (you'll have to select all the columns here)
df = df.selectExpr("existing_column_name AS existing_1", "new_column_name AS new_1")

# Changing column with sparksql functions - col and alias
from pyspark.sql.functions import col
df = df.select(col("existing_column_name").alias("existing_1"), col("new_column_name").alias("new_1"))

# Changing column with a SQL select statement
sqlContext.registerDataFrameAsTable(df, "df_table")
df = sqlContext.sql("SELECT existing_column_name AS existing_1, new_column_name AS new_1 FROM df_table")

#column drop
==============
# Remove a column from a DataFrame
df.drop('this_column')

# Remove multiple columns in a go
drop_columns = ['this_column', 'that_column']
df.select([col for col in df.columns if column not in drop_columns])

#filters:
==========
# Filter movies with avg_ratings > 7.5 and < 8.2
df.filter((F.col('avg_ratings') > 7.5) & (F.col('avg_ratings') < 8.2)).show()

# Another way to do this
df.filter(df.avg_ratings.between(7.5,8.2)).show()

# Finding info of Ace Ventura films
df.where(F.lower(F.col('title')).like("%ace%")).show()

# Another way to do this
df.where("title like '%ace%'").show()

# Using where clause in sequence
df.where(df.year != '1998').where(df.avg_ratings >= 6.0)

# Find all the films for which budget information is not available
df.where(df.budget.isNull()).show()

# Similarly, find all the films for which budget information is available
df.where(df.budget.isNotNull()).show()

#Aggregations
===========
# Year wise summary of a selected portion of the dataset
df.groupBy('year')\
          .agg(F.min('budget').alias('min_budget'),\
               F.max('budget').alias('max_budget'),\
               F.sum('revenue').alias('total_revenue'),\
               F.avg('revenue').alias('avg_revenue'),\
               F.mean('revenue').alias('mean_revenue'),\
              )\
          .sort(F.col('year').desc())\
          .show()

# Pivot to convert Year as Column name and Revenue as the value
df.groupBy().pivot('year').agg(F.max('revenue')).show()

#window functions
==============
from pyspark.sql import Window

# Rank all the films by revenue in the default ascending order
df.select("title", "year", F.rank().over(Window.orderBy("revenue")).alias("revenue_rank")).show()

# Rank year-wise films by revenue in the descending order
df.select("title", "year", F.rank().over(Window.partitionBy("year").orderBy("revenue").desc()).alias("revenue_rank")).show()




