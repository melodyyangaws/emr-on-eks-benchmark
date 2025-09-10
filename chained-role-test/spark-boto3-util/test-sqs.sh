#!/bin/bash
# SPDX-FileCopyrightText: Copyright 2021 Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: MIT-0
          # "spark.sqs.customAWSCredentialsProvider": "com.amazonaws.emr.AssumeRoleAWSCredentialsProvider",        

# export EMRCLUSTER_NAME=emr-on-eks-rss
# export AWS_REGION=us-east-1
export ACCOUNTID=$(aws sts get-caller-identity --query Account --output text)
export VIRTUAL_CLUSTER_ID=$(aws emr-containers list-virtual-clusters --query "virtualClusters[?name == '$EMRCLUSTER_NAME' && state == 'RUNNING'].id" --output text)
export EMR_ROLE_ARN=arn:aws:iam::$ACCOUNTID:role/$EMRCLUSTER_NAME-execution-role
export S3BUCKET=$EMRCLUSTER_NAME-$ACCOUNTID-$AWS_REGION
export ECR_URL="$ACCOUNTID.dkr.ecr.$AWS_REGION.amazonaws.com"

aws emr-containers start-job-run \
  --virtual-cluster-id $VIRTUAL_CLUSTER_ID \
  --name emr72-custom \
  --execution-role-arn $EMR_ROLE_ARN \
  --release-label emr-7.2.0-latest \
  --job-driver '{
  "sparkSubmitJobDriver": {
      "entryPoint": "s3://'$S3BUCKET'/my-test-sqs.py",
      "entryPointArguments":["s3://emr-on-eks-rss-021732063925-us-west-2/BLOG_TPCDS-TEST-3T-partitioned/reason","https://sqs.us-west-2.amazonaws.com/021732063925/karpenter-emr-eks-karpenter"],
      "sparkSubmitParameters": "--py-files s3://'$S3BUCKET'/aws_utils_v2.py --conf spark.driver.memory=1g --conf spark.executor.memory=2g --conf spark.executor.instances=2"}}' \
  --configuration-overrides '{
    "applicationConfiguration": [
      {
        "classification": "spark-defaults", 
        "properties": {
          "spark.network.timeout": "2000s",
          "spark.executor.heartbeatInterval": "300s",
          "spark.kubernetes.node.selector.eks.amazonaws.com/nodegroup": "c5d9b",
          "spark.kubernetes.container.image": "melodydocker/emr7.2_custom",

          "spark.hadoop.fs.s3.bucket.emr-on-eks-rss-021732063925-us-west-2.customAWSCredentialsProvider": "com.amazonaws.emr.AssumeRoleAWSCredentialsProvider", 
          "spark.kubernetes.driverEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN": "arn:aws:iam::021732063925:role/emr-on-eks-client-a-role",
          "spark.executorEnv.ASSUME_ROLE_CREDENTIALS_ROLE_ARN": "arn:aws:iam::021732063925:role/emr-on-eks-client-a-role"
          
      }}
    ], 
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": {"logUri": "s3://'$S3BUCKET'/elasticmapreduce/emr-containers"}}}'
