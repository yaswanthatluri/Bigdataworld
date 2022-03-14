import boto3
import json
import time
import ast
from pyspark.sql.functions import monotonically_increasing_id, lit, when
import pyspark.sql.functions as fn
from pyspark.sql import DataFrame
from pyspark.sql.types import FloatType, BooleanType, StringType
from pyspark.sql.functions import col, when
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import when, input_file_name, col

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime
from datetime import timedelta

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

logger = glue_context.get_logger()
job = Job(glue_context)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'aws_region', 'zone', 'source_bucket', 'LOG_LEVEL','backup_bucket', 'source_prefixes',
                                     'partition_key_1', 'partition_key_2', 'partition_value_1','file_type','expiry_days',
                                     's3_backup_enabled', 'scramble_attribute_name', 'scramble_attribute_value_list','part_date_list_str','job_run_id'])

job.init(args['JOB_NAME'], args)
log_level = args['LOG_LEVEL']
aws_region = args['aws_region']
zone = args['zone']
partition_key_1 = args['partition_key_1']
partition_key_2 = args['partition_key_2']
partition_value_1 = args['partition_value_1']
s3_bucket = args['source_bucket']
s3_bkp_bucket = args['backup_bucket']
s3_prefix_value = args['source_prefixes']
file_type = args['file_type']
expiry_days = int(args['expiry_days'])
s3_backup_enabled = args['s3_backup_enabled']
attribute_name = args['scramble_attribute_name']
attribute_value_list = args['scramble_attribute_value_list'].split(",")
# the input should be in the form of string format list : "['2022-01-05', '2022-01-03','2022-02-21']"
part_date_list_str = args['part_date_list_str']
job_run_id = args['job_run_id']


s3_client = boto3.client('s3', region_name=aws_region)
sns_client = boto3.client('sns',region_name=aws_region )
#dictionary for saving the validation results
validation_dict = {}
validation_dict2 = {}

topic_arn = 'arn:aws:sns:us-east-1:434153611018:uv-redshift-gz-us-east-1-email-sns'
date_partitions = ast.literal_eval(part_date_list_str)
data_set_name = s3_prefix_value.split("/")[-1]

def s3_path_generator(date_partitions,s3_bucket,s3_prefix_value,partition_key_1,partition_value_1,partition_key_2):
    dist_partitions_list = []
    for date in date_partitions:
        data_s3_path = f's3://{s3_bucket}/{s3_prefix_value}/{partition_key_1}={partition_value_1}/{partition_key_2}={date}'
        dist_partitions_list.append(data_s3_path)
    return dist_partitions_list

s3_path_list = s3_path_generator(date_partitions,s3_bucket,s3_prefix_value,partition_key_1,partition_value_1,partition_key_2)
"""
['s3://uvl-gn-i-s3-gnannobucketc81dfdc1-4cele1g2xbtl/raw/health_portal/profile/partition_version=v1/dt=2022-01-05', 
's3://uvl-gn-i-s3-gnannobucketc81dfdc1-4cele1g2xbtl/raw/health_portal/profile/partition_version=v1/dt=2022-01-03', 
's3://uvl-gn-i-s3-gnannobucketc81dfdc1-4cele1g2xbtl/raw/health_portal/profile/partition_version=v1/dt=2022-02-21']
"""
def bkp_partition_validation(s3_bucket,job_run_id,s3_prefix_value,partition_key_1,partition_value_1,partition_key_2,date_partitions,file_type):
    for loop_dt_str in date_partitions:

        data_s3_path = f's3://{s3_bucket}/{s3_prefix_value}/{partition_key_1}={partition_value_1}/{partition_key_2}={loop_dt_str}/'

        #backup s3 path is from the prefix: "scrambler_backup/"
        backup_s3_path = f's3://{s3_bkp_bucket}/scrambler_backup/{job_run_id}/{s3_prefix_value}/{partition_key_1}={partition_value_1}/{partition_key_2}={loop_dt_str}/'

        # Read Data from Back-up path
        backup_data_df = glue_context.read.format(file_type).load(backup_s3_path)

        try:
            #read the data from reg prefix
            data_df = glue_context.read.format(file_type).load(data_s3_path)
        except Exception as e:
            if 'Path does not exist' in (str(e)):
                s3_client.put_object(Bucket=s3_bucket,
                                    Key=f'{s3_prefix_value}/{partition_key_1}={partition_value_1}/{partition_key_2}={loop_dt_str}')
                backup_data_df.write.format(file_type).mode("overwrite").save(data_s3_path)
                data_df = glue_context.read.format(file_type).load(data_s3_path)
            elif 'Unable to infer schema' in (str(e)):
                backup_data_df.write.format(file_type).mode("overwrite").save(data_s3_path)
                data_df = glue_context.read.format(file_type).load(data_s3_path)
            else:
                logger.info("unable to read the data_df")


        #getting the counts of data_df and the backup_df
        data_df_count = data_df.count()
        back_up_count = backup_data_df.count()

        if back_up_count > data_df_count:
            logger.info(
                f'{data_set_name}-data_df_count is {data_df_count} | back_up_count is {back_up_count} | for date {loop_dt_str}')
            backup_data_df.write.parquet(data_s3_path, mode="overwrite")
            validation_dict['trigger_scrambler_glue_job'] = "yes"
        elif back_up_count == data_df_count:
            logger.info("the counts of backup_df and the data_df is same")
            validation_dict['trigger_scrambler_glue_job'] = "no"
        else:
            pass
    return validation_dict

def Profile_scrambler_check_validation(s3_path_list, attribute_value_list, attribute_name, validation_dict2, file_type):
    for attribute in attribute_value_list:
        df_raw = spark.read.format(file_type).option("mergeSchema", "true").load(path=s3_path_list).withColumn(
            "input_file", input_file_name())
        scramble_df = df_raw.filter(col(attribute_name).isin(attribute)).select("input_file")
        scramble_df_count = scramble_df.count()
        if scramble_df_count == 0:
            validation_dict2[attribute] = f"validation2 passed for attribute-{attribute}"
        elif scramble_df_count != 0:
            distinct_partitions_path = scramble_df.rdd.map(lambda row: row[0].rsplit('/', 1)[0]).distinct().collect()
            validation_dict2[attribute] = distinct_partitions_path
    return validation_dict2

# Publish to topic
def send_results_to_email(sns_client,topic_arn,message_text,email_subject):
    sns_client.publish(TopicArn=topic_arn,
            Message=message_text,
            Subject=email_subject)


if __name__ == '__main__':
    """
            This Glue Job performs below actions:
            - Gets parameters from the previous glue job and validates the o/p of previous job
            - Glue job validates the count of profile id in the actual location[if the value is not zero: the data is not scrambled]
            - all the values will be coming from previous glue job
            - validation2 checks for the attribute's in the attribute_value_list
    """
    if len(date_partitions) >0:
        if s3_backup_enabled == 'yes':
            validation_dict1 = bkp_partition_validation(s3_bucket,job_run_id,s3_prefix_value,partition_key_1,partition_value_1,partition_key_2,date_partitions,file_type)
            logger.info(f'the result of bkp_partition_validation is {validation_dict1}')
            
            #the output will be like below
            """
            22/03/09 05:26:18 INFO GlueLogger: the result of bkp_partition_validation is {'trigger_scrambler_glue_job': 'no'}
            """
            

    if len(s3_path_list) >0:
        validation_dict2 = Profile_scrambler_check_validation(s3_path_list,attribute_value_list,attribute_name,validation_dict2,file_type)
        logger.info(f'the result of Profile_scrambler_check_validation is {validation_dict2}')
        
        """
            #the output of teh above function will be
            22/03/09 05:26:31 INFO GlueLogger: the result of Profile_scrambler_check_validation is {'184': 'validation2 passed for attribute-184', 'ADDRESS': ['s3://uvl-gn-i-s3-gnannobucketc81dfdc1-4cele1g2xbtl/raw/health_portal/profile/partition_version=v1/dt=2022-02-22', 's3://uvl-gn-i-s3-gnannobucketc81dfdc1-4cele1g2xbtl/raw/health_portal/profile/partition_version=v1/dt=2022-02-23', 's3://uvl-gn-i-s3-gnannobucketc81dfdc1-4cele1g2xbtl/raw/health_portal/profile/partition_version=v1/dt=2022-02-24'], 'MONEY': ['s3://uvl-gn-i-s3-gnannobucketc81dfdc1-4cele1g2xbtl/raw/health_portal/profile/partition_version=v1/dt=2022-02-22', 's3://uvl-gn-i-s3-gnannobucketc81dfdc1-4cele1g2xbtl/raw/health_portal/profile/partition_version=v1/dt=2022-02-23', 's3://uvl-gn-i-s3-gnannobucketc81dfdc1-4cele1g2xbtl/raw/health_portal/profile/partition_version=v1/dt=2022-02-24']}
        
        """




