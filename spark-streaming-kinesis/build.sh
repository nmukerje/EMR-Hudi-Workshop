#!/bin/bash
S3_BUCKET=$1
if [ -z "$S3_BUCKET" ] 
then
  S3_BUCKET=aksh-test-versioning
fi

sbt clean package
sbt assembly
aws s3 cp target/scala-2.11/Spark-Structured-Streaming-Kinesis-Hudi-assembly-1.0.jar s3://$S3_BUCKET/