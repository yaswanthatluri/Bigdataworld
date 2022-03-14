import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

logger = glue_context.get_logger()
job = Job(glue_context)

input_path = "s3://"
output_path = "s3://"

#loading the test csv file for our operations
#renaming multiple columns in spark
df1 = spark.read.format("csv").option("header", "true").load(input_path)
df1 = df1.withColumnRenamed("Semiotic Class","semioticclass").withColumnRenamed("Input Token","inputtoken").withColumnRenamed("Output Token","outputtoken")

#schema
schema = df1.printSchema()
logger.info(f'the schema before convertion is {schema}')

#df1.write.partitionBy("dt").parquet('output_path')

#df1.repartition(60)

#df1.write.parquet(output_path, mode="overwrite", compression='snappy')

job.commit()
