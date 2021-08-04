#spark job to load the data and in partitions

from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession \
    .builder \
    .appName("loading partition data in calender") \
    .enableHiveSupport() \
    .getOrCreate()

df1 = spark.read.format("csv").option("header", "true").load("s3://yash-east/calender-data/calendar.csv")

df1.printSchema()

df1.write.partitionBy('year').format('parquet').mode('overwrite').save("s3://yash-bootstrapactions/calender-output/rawoutput")



#spark program to count the number of files in a partition
#it returns a dictionary with teh number of files in each partition after looping the partitions.


import json
import time
import ast
from pyspark.sql.functions import monotonically_increasing_id, lit, when
import pyspark.sql.functions as fn
from pyspark.sql.types import FloatType, BooleanType, StringType
from pyspark.sql.functions import col, when

date_list = ['year=1960','year=1961','year=1962']
red_input_path = "yash-bootstrapactions/calender-output/rawoutput"
tgt_file_count_s3_path = "s3://yashcdk-bucket/calender-final-output/finaloutoutput"
s3_file_count_lst = []

for loop_dt_str in date_list:
        data_path = f's3://{red_input_path}/{loop_dt_str}/'
    
        df1 = spark.read.parquet(data_path)
        s3_files = df1.withColumn("input_file_name", fn.input_file_name()).select('input_file_name').distinct().collect()
        s3_file_count =len(s3_files)
        s3_file_count_dict = {'partition_date': loop_dt_str, 'file_count': s3_file_count}
        s3_file_count_lst.append(s3_file_count_dict)
        print(s3_file_count_lst)

file_count_df = spark.createDataFrame(s3_file_count_lst)
file_count_df = file_count_df.repartition(1)
file_count_df.write.csv(tgt_file_count_s3_path, mode="overwrite",header="true")



