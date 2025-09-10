from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct
import boto3
import sys
from botocore.exceptions import ClientError
import os
from botocore.session import Session
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def debug_aws_credentials(context=""):

    try:
        print(f"\n=== AWS Credential Debug Information ({context}) ===")
        
        # Get current session and credentials
        session = boto3.Session()
        credentials = session.get_credentials()
        
        # Print environment variables
        print("\nAWS Environment Variables:")
        env_vars = ['AWS_PROFILE', 'AWS_DEFAULT_PROFILE', 'AWS_CONFIG_FILE', 
                   'AWS_SHARED_CREDENTIALS_FILE', 'AWS_ROLE_ARN', 
                   'AWS_WEB_IDENTITY_TOKEN_FILE', 'AWS_SDK_LOAD_CONFIG']
        for var in env_vars:
            print(f"{var}: {os.environ.get(var, 'Not Set')}")

        # Print credential info
        if credentials:
            print("\nCurrent Credentials:")
            print(f"Access Key ID: {credentials.access_key[:5]}..." if credentials.access_key else "No Access Key")
            print(f"Secret Key: {credentials.secret_key[:5]}..." if credentials.secret_key else "No Secret Key")
            if credentials.token:
                print(f"Session Token: {credentials.token[:20]}...")

        # Get current identity using STS
        sts = session.client('sts')
        identity = sts.get_caller_identity()
        print("\nCurrent Identity (STS):")
        print(f"ARN: {identity['Arn']}")
        print(f"Account: {identity['Account']}")
        print(f"UserId: {identity['UserId']}")

        # Check if using web identity token
        if os.environ.get('AWS_WEB_IDENTITY_TOKEN_FILE'):
            token_path = os.environ.get('AWS_WEB_IDENTITY_TOKEN_FILE')
            print(f"\nWeb Identity Token File: {token_path}")
            if os.path.exists(token_path):
                print("Token file exists")
                try:
                    with open(token_path, 'r') as f:
                        token_content = f.read()
                    print(f"Token content (first 50 chars): {token_content[:50]}...")
                except Exception as e:
                    print(f"Error reading token file: {e}")
            else:
                print("Token file does not exist")

        print("\n=====================================")

    except Exception as e:
        print(f"Error in credential debugging: {str(e)}")

def send_partition(partition):
    print(f"\nInitializing SQS client for partition...")
    debug_aws_credentials("Before SQS operations in partition")
    
    try:
        sqs = boto3.client('sqs', region_name='us-west-2')        
        
        results = []
        results.append(f"Caller Identity: {boto3.client('sts').get_caller_identity()}")
        
        for i, row in enumerate(partition, 1): 
            try:
                response = sqs.send_message(
                    QueueUrl=SQS_URL,
                    MessageBody=row.value
                )
                results.append(f"Sent message {i} - MessageId: {response['MessageId']}")
            
            except ClientError as e:
                debug_aws_credentials(f"After SQS error in message {i}")
                results.append(f"Failed: {e} | Message: {row.value}")
                
        return results
    except Exception as e:
        debug_aws_credentials("After partition failure")
        return [f"Partition failed: {str(e)}"]

def main():
    print("=== Starting Spark Session ===")
    debug_aws_credentials("At application start")
    
    global SQS_URL  # Make SQS_URL accessible in send_partition
    S3_FILE = sys.argv[1]
    SQS_URL = sys.argv[2]

    spark = SparkSession.builder.appName("irsa-poc").getOrCreate()

    # 1. Read data from S3
    print("\n=== Reading from S3 ===")
    debug_aws_credentials("Before S3 read")
    try:
        df = spark.read.parquet(S3_FILE)
    except Exception as e:
        print(f"Error reading S3 data: {e}")
        debug_aws_credentials("After S3 error")
        spark.stop()
        return
    
    # 2. Convert each row to JSON string
    print("Converting rows to JSON...")
    json_df = df.select(to_json(struct("*")).alias("value"))    

    print("=== Sample JSON Output ===")
    json_df.show(5, truncate=False)

    # 3. Send to SQS
    print("\n=== Starting boto3 connection ===")
    debug_aws_credentials("Before starting RDD operations")
    
    results = json_df.rdd.mapPartitions(send_partition).collect()
    for msg in results:
        print(msg)
        
    print("\n=== Job Completed ===")
    debug_aws_credentials("At job completion")


if __name__ == "__main__":
    # Enable boto3 debug logging
    boto3.set_stream_logger('', logging.DEBUG)
    
    print("Script started")
    main()
    print("Script finished")
