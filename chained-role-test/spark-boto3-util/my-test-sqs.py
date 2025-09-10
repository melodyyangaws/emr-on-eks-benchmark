from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct
import boto3
import sys,os,json
from botocore.exceptions import ClientError

# ====== extra line ====== 
from aws_utils_v2 import get_assume_role
# ====== extra line ======

def main():
    print("=== Starting Spark Session ===")
    S3_FILE=sys.argv[1]
    SQS_URL=sys.argv[2]

    spark = SparkSession.builder.appName("irsav1-poc").getOrCreate() 

    # ====== extra line ======
    boto3_session = get_assume_role(spark)
    # ====== extra line ======

    # 1. Read data from S3
    try:
        df = spark.read.parquet(S3_FILE)
    except Exception as e:
        print(f"Error reading S3 data: {e}")
        spark.stop()
        return
    
    # 2. Convert each row to JSON string
    print("Converting rows to JSON...")
    json_df = df.select(to_json(struct("*")).alias("value"))    

    print("=== Sample JSON Output ===")
    json_df.show(5, truncate=False)

    # 3. Send to SQS
    def send_partition(partition, aws_session):
        print(f"\nInitializing SQS client for partition...")
        try:
            # ====== use the boto3 session  ======
            sqs = aws_session.client('sqs', region_name='us-west-2')        
            # ====== use the boto3 session  ======
            
            results = []
            results.append(f"Caller Identity: {aws_session.client('sts').get_caller_identity()}")
            
            for i, row in enumerate(partition, 1): 
                try:
                    response=sqs.send_message(
                        QueueUrl=SQS_URL,
                        MessageBody=row.value

                    )
                    results.append(f"Sent message {i} - MessageId: {response['MessageId']}")
                
                except ClientError as e:
                    results.append(f"Failed: {e} | Message: {row.value}")
            return results
        except Exception as e:
            return [f"Partition failed: {str(e)}"]

    print("\n=== Starting SQS Submission ===")
    results = json_df.rdd.mapPartitions(lambda partition: send_partition(partition, boto3_session)).collect()
    for msg in results:
        print(msg)
    print("\n=== Job Completed ===")


if __name__ == "__main__":
    print("Script started")
    main()
    print("Script finished")