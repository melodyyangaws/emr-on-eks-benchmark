#!/bin/bash
# SPDX-FileCopyrightText: Copyright 2021 Amazon.com, Inc. or its affiliates.
# SPDX-License-Identifier: MIT-0

# export EMRCLUSTER_NAME=emr-on-eks-test
export REGION=us-west-2
export ACCOUNTID=$(aws sts get-caller-identity --query Account --output text)
export S3BUCKET=$EMRCLUSTER_NAME-$ACCOUNTID-$REGION
export VIRTUAL_CLUSTER_ID=$(aws emr-containers list-virtual-clusters --query "virtualClusters[?name == '$EMRCLUSTER_NAME' && state == 'RUNNING'].id" --output text)
export EMR_ROLE_ARN=arn:aws:iam::$ACCOUNTID:role/$EMRCLUSTER_NAME-execution-role
export ROLE_2_ARN=arn:aws:iam::${ACCOUNTID}:role/emr-on-eks-client-a-role
export ECR_URL=$ACCOUNTID.dkr.ecr.$REGION.amazonaws.com

aws emr-containers start-job-run \
  --virtual-cluster-id $VIRTUAL_CLUSTER_ID \
  --name S3ListObjects-v1 \
  --execution-role-arn $EMR_ROLE_ARN \
  --release-label emr-6.15.0-latest \
  --job-driver '{
  "sparkSubmitJobDriver": {
      "entryPoint": "s3://emr-on-eks-rss-'$ACCOUNTID'-us-west-2/S3ListObjects_v1-debug.jar",
      "entryPointArguments":["emr-on-eks-rss-'$ACCOUNTID'-us-west-2","BLOG_TPCDS-TEST-3T-partitioned/reason"],
      "sparkSubmitParameters": "--class com.sf.S3ListObjects --conf spark.executor.cores=1 --conf spark.executor.instances=1"}}' \
  --configuration-overrides '{
    "applicationConfiguration": [
      {
        "classification": "spark-defaults", 
        "properties": {
          "spark.network.timeout": "2000s",
          "spark.executor.heartbeatInterval": "300s",
          "spark.kubernetes.container.image": "'$ECR_URL'/spark:emr6.15_custom",

          "spark.kubernetes.driverEnv.WEB_IDENTITY_ROLE_ARN": "'$EMR_ROLE_ARN'",
          "spark.executorEnv.WEB_IDENTITY_ROLE_ARN":  "'$EMR_ROLE_ARN'",
          "spark.kubernetes.driverEnv.ROLE_2_ARN": "'${ROLE_2_ARN}'",
          "spark.executorEnv.ROLE_2_ARN": "'${ROLE_2_ARN}'",
          "spark.kubernetes.driverEnv.REGION": "'${REGION}'",
          "spark.executorEnv.REGION": "'${REGION}'"
      }},
      {
        "classification": "spark-log4j",
        "properties": {
          "rootLogger.level" : "DEBUG"
          }
      }
    ], 
    "monitoringConfiguration": {
      "s3MonitoringConfiguration": {"logUri": "s3://'$S3BUCKET'/elasticmapreduce/emr-containers"},
      "cloudWatchMonitoringConfiguration": {
        "logGroupName": "/emr-on-eks/chain-role-test",
        "logStreamNamePrefix": "pure-java"}
    }}'
