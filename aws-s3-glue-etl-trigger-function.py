import json
import boto3
import os

def lambda_handler(event, context):
    
    print("Lambda Function Got Triggered")
    
    # Reading configurations from the environment variables
    AWS_ACCESS_KEY_ID = os.environ["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = os.environ["aws_secret_access_key"]
    REGION = os.environ["aws_region"]
    GLUE_JOB_NAME = os.environ["glue_job_name"]
    
    # Getting the bucket name and filename from the s3 event
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    file_name = event["Records"][0]["s3"]["object"]["key"]
    fully_qualified_file_path = "s3://{}/{}".format(bucket_name,file_name)
    params = {"--source_file_path" : fully_qualified_file_path}
    
    print("Bucket Name : ", bucket_name)
    print("File Name : ",file_name)
    print("Fully Qualified File Path : ",fully_qualified_file_path)
    
    # Creating a glue client and starting a glue job with required params
    glue_client = boto3.client("glue",region_name=REGION,aws_access_key_id=AWS_ACCESS_KEY_ID,
                               aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    try:
        response = glue_client.start_job_run(JobName=GLUE_JOB_NAME,
                                             Arguments = params)
    except Exception as error:
        print("Exception : ", error)
    
    