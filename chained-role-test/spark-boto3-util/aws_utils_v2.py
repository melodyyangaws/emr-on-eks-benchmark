# aws_utils_v2.py
import boto3
import os
from botocore.exceptions import ClientError
from py4j.java_gateway import java_import


def get_assume_role(spark_session):
    try:
        jvm = spark_session.sparkContext._jvm
        # Import the Java class
        java_import(jvm, "com.amazonaws.emr.AssumeRoleAWSCredentialsProvider")
        credentials_provider = jvm.AssumeRoleAWSCredentialsProvider()
        cred = credentials_provider.getCredentials()

        session = boto3.Session(
            aws_access_key_id=cred.getAWSAccessKeyId(),
            aws_secret_access_key=cred.getAWSSecretKey(),
            aws_session_token=cred.getSessionToken()
        )
        return session

    except ClientError as e:
        print(f"Failed to retrieve assume role: {e}")
        raise