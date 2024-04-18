import json
import boto3
import re
import time

client = boto3.client('s3')
sfn_client = boto3.client('stepfunctions')
s3 = boto3.resource('s3')

SOURCE_BUCKET = "nyc-tlc"
FILE_PREFIX = "trip data/fhvhv_tripdata"
RAW_BUCKET = "raw-data-group-b"
TARGET_FOLDER = "fhvhv_trip_data"
YEAR_MONTH_PATTERN = r"(\d{4}-\d{2})"

STEP_FUNCTION_NAME = "arn:aws:states:us-east-1:211125553029:stateMachine:FHVHV-TRIP-DATA-PIPELINE"

def divide_chunks(l, n): 
    # looping till length l 
    for i in range(0, len(l), n):  
        yield l[i:i + n] 

def list_files():
    print("LISTING FILES")
    file_list = []
    
    response = client.list_objects_v2(
        Bucket=SOURCE_BUCKET,
        Prefix=FILE_PREFIX
    )
    
    if response:
        file_list = response["Contents"]
        
    return file_list
        
def copy_file(raw_file_list):
    
    output_file_list = []
    for raw_file in raw_file_list:
        source_file_key = raw_file["Key"]
        print(f"COPYING RAW FILE:{source_file_key}")
        
        copy_source = {
          'Bucket': SOURCE_BUCKET,
          'Key': source_file_key
        }
        
        target_file_key = f"{TARGET_FOLDER}/{source_file_key.split('/')[-1]}"
        bucket = s3.Bucket(RAW_BUCKET)
        bucket.copy(copy_source, target_file_key)
        print(f"FILE COPIED TO: {RAW_BUCKET}/{target_file_key}")
        output_file_list.append(f"{RAW_BUCKET}/{target_file_key}")
    
    return output_file_list
        
def trigger_step_function(raw_file_list):
    step_config = []
    raw_file_list.sort()
    for file_name in raw_file_list[51:]:
        print(f"FILE NAME: {file_name}")
        match = re.findall(YEAR_MONTH_PATTERN, file_name, 0)
        year_month = match[0]
        print(f"YEAR-MONTH: {year_month}")
        
        config = {
            "file_name": f"s3://{file_name}",
            "bronze_table": "bronze.fhvhv_trip_data",
            "silver_table": "silver.fhvhv_trip_data",
            "load_date_filter": "none",
            "year_month_filter": year_month,
            "write_mode": "append"
        }
        
        step_config.append(config)
    
    step_config_chunks = list(divide_chunks(step_config, 5)) 
    
    for item in step_config_chunks:
        response = sfn_client.start_execution(
            stateMachineArn=STEP_FUNCTION_NAME,
            input=json.dumps({"to_ingest":item})
        )
        time.sleep(2)
        
        print(f"RESPONSE: {response}")
        
        

        
        
        
def lambda_handler(event, context):
    print("INGEST RAW DATA")
    
    file_list = list_files()
    print(f"FILE_LIST:{file_list}")
    
    output_file_list = copy_file(file_list)
    
    trigger_step_function(output_file_list)
    
    print("DONE")
    return {
        'statusCode': 200,
        'body': json.dumps('INGESTION RAW DATA DONE')
    }
