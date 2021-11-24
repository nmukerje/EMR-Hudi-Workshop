
# EMR Prerequisite
1. Create EMR cluster with Spark, Hive , Hadoop, Tez and Livy enabled. Refer the template at cloudformation/hudi-workshop-emr-spark.yaml
2. SSH to master node and execute command to copy Jars 

```
mkdir -p /home/hadoop/rpms
cd /home/hadoop/rpms

# Copy log4j settings
sudo aws s3 cp s3://emr-workshops-us-west-2/hudi/scripts/log4j.properties /etc/spark/conf/

```
# Spark Submit Prerequisite
1. Build and copy jar by running spark-streaming-kinesis/build.sh. ./build.sh <S3-Bucket-Name>
2. SSH to master node and copy jar which was pushed to S3.
    
```
   aws s3 cp s3://<build-jar-bucket>/Spark-Structured-Streaming-Kinesis-Hudi-assembly-1.0.jar .   
```

# Use Case 1 - Events Published to Kinesis with simulation of later arriving events
## Message Content pushed to the topic
timestamp has epoch value in seconds. 

```
{
   "tradeId":"211124204181756",
   "symbol":"GOOGL",
   "quantity":"39",
   "price":"39",
   "timestamp":1637766663,
   "description":"Traded on Wed Nov 24 20:41:03 IST 2021",
   "traderName":"GOOGL trader",
   "traderFirm":"GOOGL firm"
}

```

## Spark Submit 
SSH to master node and then run the spark submit command.

```
spark-submit --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" --conf "spark.sql.hive.convertMetastoreParquet=false" --conf "spk.dynamicAllocation.maxExecutors=10" --jars /usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar  --packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.4.5,com.qubole.spark:spark-sql-kinesis_2.11:1.2.0_spark-2.4 --class kinesis.hudi.latefile.SparkKinesisConsumerHudiProcessor Spark-Structured-Streaming-Kinesis-Hudi-assembly-1.0.jar <bucket-name>  <stream-name> <region> <COW/MOR>
```
Example
```
spark-submit --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" --conf "spark.sql.hive.convertMetastoreParquet=false" --conf "spk.dynamicAllocation.maxExecutors=10" --jars /usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar  --packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.4.5,com.qubole.spark:spark-sql-kinesis_2.11:1.2.0_spark-2.4 --class kinesis.hudi.latefile.SparkKinesisConsumerHudiProcessor Spark-Structured-Streaming-Kinesis-Hudi-assembly-1.0.jar aksh-firehose-test hudi-stream-ingest us-west-2 COW
	
```

## Spark Shell
Run the shell with command below and copy paste from  spark-streaming-kinesis/src/main/scala/kinesis/hudi/latefile/spark-shell-commands.txt

```
spark-shell --jars hdfs:///hudi-spark-bundle.jar,hdfs:///spark-avro.jar,hdfs:///httpclient-4.5.9.jar --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.hive.convertMetastoreParquet=false' --packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.4.5,com.qubole.spark:spark-sql-kinesis_2.11:1.2.0_spark-2.4
```

# Use Case 2 - CDC Published to Kinesis
    
## Message Content pushed to the topic
DMS publishes the changes to Kineiss 
```
{
		"data": {
		"LINE_ID": 144611,
		"LINE_NUMBER": 1,
		"ORDER_ID": 11363,
		"PRODUCT_ID": 927,
		"QUANTITY": 142,
		"UNIT_PRICE": 36,
		"DISCOUNT": 3,
		"SUPPLY_COST": 15,
		"TAX": 0,
		"ORDER_DATE": "2015-10-17"
		},
		"metadata": {
		"timestamp": "2021-11-19T13:24:43.297344Z",
		"record-type": "data",
		"operation": "update",
		"partition-key-type": "schema-table",
		"schema-name": "salesdb",
		"table-name": "SALES_ORDER_DETAIL",
		"transaction-id": 47330445004
		}
} 
```

## Spark Submit 

SSH to master node and then run the spark submit command.
```
spark-submit --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" --conf "spark.sql.hive.convertMetastoreParquet=false" --conf "spk.dynamicAllocation.maxExecutors=10" --jars hdfs:///httpclient-4.5.9.jar,hdfs:///hudi-spark-bundle.jar,hdfs:///spark-avro.jar --packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.4.5,com.qubole.spark:spark-sql-kinesis_2.11:1.2.0_spark-2.4 --class kinesis.hudi.SparkKinesisConsumerHudiProcessor Spark-Structured-Streaming-Kinesis-Hudi-assembly-1.0.jar <bucket-name>  <stream-name> <region> <COW/MOR>

```
## Spark Shell

```
spark-shell --jars hdfs:///hudi-spark-bundle.jar,hdfs:///spark-avro.jar,hdfs:///httpclient-4.5.9.jar --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.hive.convertMetastoreParquet=false' --packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.4.5,com.qubole.spark:spark-sql-kinesis_2.11:1.2.0_spark-2.4

```


# Use Case 3 - CDC Published to S3. S3 event triggered Lambda pushes file path to Kinesis. 
## Message Content pushed to the topic
The filePath here is the path to the file which got added to S3 by DMS. An S3 event gets published which is consumed by Lambda. The lambda then pushes the event below to the Kinesis stream which the file path of the file that got ingested. 
```
{
    "filePath": "s3://<bucket-name>/dms-full-load-path/salesdb/SALES_ORDER_DETAIL/20211118-100428844.parquet"
}
```

## Spark Submit 
    
    
```
spark-submit --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" --conf "spark.sql.hive.convertMetastoreParquet=false" --conf "spk.dynamicAllocation.maxExecutors=10" --jars hdfs:///httpclient-4.5.9.jar,hdfs:///hudi-spark-bundle.jar,hdfs:///spark-avro.jar --packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.4.5,com.qubole.spark:spark-sql-kinesis_2.11:1.2.0_spark-2.4 --class kinesis.hudi.SparkKinesisFilePathConsumerHudiProcessor Spark-Structured-Streaming-Kinesis-Hudi-assembly-1.0.jar <bucket-name>  <stream-name> <region> <COW/MOR>
```

## Spark Shell
```

spark-shell --jars hdfs:///hudi-spark-bundle.jar,hdfs:///spark-avro.jar,hdfs:///httpclient-4.5.9.jar --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.hive.convertMetastoreParquet=false' --packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.4.5,com.qubole.spark:spark-sql-kinesis_2.11:1.2.0_spark-2.4
```
