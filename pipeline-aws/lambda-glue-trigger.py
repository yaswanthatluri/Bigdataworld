import json
import boto3

def lambda_handler(event, context):
    client = boto3.client('glue', region_name='us-east-1')
    client.start_job_run(
               JobName = 'gn_scrambler_validation1',
               Arguments = {
               "--aws_region":"us-east-1",
               "--zone":"green",
               "--partition_key_1": "partition_version",
               "--partition_key_2": "dt",
               "--partition_value_1": "v1",
               "--source_bucket":"uvl-gn-i-s3-gnannobucketc81dfdc1-4cele1g2xbtl",
               "--backup_bucket": "uvl-gn-i-s3-gnbkpbucket6430036c-3snxltrzicj9",
               "--source_prefixes": "raw/health_portal/profile",
               "--file_type":"parquet",
               "--expiry_days":"3",
               "--s3_backup_enabled":"yes",
               "--scramble_attribute_name":"semioticclass",
               "--job_run_id":"jr1",
               "--scramble_attribute_value_list": "184,ADDRESS,MONEY",
               "--part_date_list":"['2022-02-22','2022-02-23','2022-02-24']",
               "--LOG_LEVEL": "INFO"} )
    print("glue job triggered successfully")
    return {
        'statusCode': 200,
        'body': json.dumps('Glue job Triggered!')
    }

    
    