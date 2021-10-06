#convert the csv file to parquet and write it in partitions
import json
import time
import ast
from pyspark.sql.functions import monotonically_increasing_id, lit, when
import pyspark.sql.functions as fn
from pyspark.sql.types import FloatType, BooleanType, StringType
from pyspark.sql.functions import col, when

input_path = "s3://yash-east/covid-data/covid_vaccination_vs_death_ratio.csv"
df1 = spark.read.format("csv").option("inferSchema","true").option("header","true").load(input_path)

df2=df1.select([col(c).cast("string") for c in df1.columns])
df2.write.partitionBy("date").parquet("s3://yash-east/covid-parquet/version=v1")


#Convert all the columns in df as string and read all the partitions data 
#Read all the partitions data at a same time

import json
import time
import ast
from pyspark.sql.functions import monotonically_increasing_id, lit, when
import pyspark.sql.functions as fn
from pyspark.sql.types import FloatType, BooleanType, StringType
from pyspark.sql.functions import col, when

input_path = "s3://yash-east/covid-parquet/version=v1/"
df1 = spark.read.format("parquet").option("mergeSchema", "true").load(input_path)

df1.printSchema()

df2=df1.select([col(c).cast("string") for c in df1.columns])

df2.printSchema()

df2.write.partitionBy("date").parquet("s3://yash-east/covid-partitioned-final/verion=v1")
