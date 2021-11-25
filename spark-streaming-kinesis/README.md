
# Prerequisites
## EMR Prerequisites
1. Create EMR cluster with Spark, Hive and Hadoop enabled. Refer the template at [cloudformation/hudi-workshop-emr-spark.yaml](../cloudformation/hudi-workshop-emr-spark.yaml) for EMRSparkHudiCluster.
2. SSH to master node and execute command to update log level to [log4j.rootCategory=WARN,console] --this is an optional step 

```
vi /etc/spark/conf/log4j.properties 

```
## Spark Submit Prerequisite
1. Build and copy jar by running spark-streaming-kinesis/build.sh. 
```
./build.sh <S3-Bucket-Name>
```

2. SSH to master node and copy jar which was pushed to S3.
    
```
   aws s3 cp s3://<S3-Bucket-Name>/Spark-Structured-Streaming-Kinesis-Hudi-assembly-1.0.jar .   
```

# Use Case 1 - Events Published to Kinesis with simulation of late arriving events
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
## Spark Scala Code
[kinesis.hudi.latefile.SparkKinesisConsumerHudiProcessor](src/main/scala/kinesis/hudi/latefile/SparkKinesisConsumerHudiProcessor.scala)

## Spark Submit 
SSH to master node and then run the spark submit command.

```
spark-submit \
--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
--conf "spark.sql.hive.convertMetastoreParquet=false" \
--conf "spk.dynamicAllocation.maxExecutors=10" \
--jars /usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar \
--packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.4.5,com.qubole.spark:spark-sql-kinesis_2.11:1.2.0_spark-2.4 \
--class kinesis.hudi.latefile.SparkKinesisConsumerHudiProcessor Spark-Structured-Streaming-Kinesis-Hudi-assembly-1.0.jar \
<bucket-name>  <stream-name> <region> <COW/MOR> <table_name>
```
Example
```
spark-submit \
--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
--conf "spark.sql.hive.convertMetastoreParquet=false" \
--conf "spk.dynamicAllocation.maxExecutors=10" \
--jars /usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar \
--packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.4.5,com.qubole.spark:spark-sql-kinesis_2.11:1.2.0_spark-2.4 \
--class kinesis.hudi.latefile.SparkKinesisConsumerHudiProcessor Spark-Structured-Streaming-Kinesis-Hudi-assembly-1.0.jar \
aksh-firehose-test hudi-stream-ingest us-west-2 COW trade_event_late_simulation
	
	
```

## Spark Shell
Run the shell with command below and copy paste code from   [kinesis.hudi.latefile.SparkKinesisConsumerHudiProcessor](src/main/scala/kinesis/hudi/latefile/SparkKinesisConsumerHudiProcessor.scala). The code that needs to be copied is between  (Spark Shell ---Start ) and (Spark Shell ---End ). Also ensure that the you hard code the paremeters like s3_bucket, streamName, region ,tableType and hudiTableNamePrefix.  

```
spark-shell \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.hive.convertMetastoreParquet=false' \
--jars /usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar \
--packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.4.5,com.qubole.spark:spark-sql-kinesis_2.11:1.2.0_spark-2.4
```

# Use Case 2 - Consume CDC events Published to Kinesis by DMS
    
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
## Spark Scala Code
[kinesis.hudi.SparkKinesisConsumerHudiProcessor](src/main/scala/kinesis/hudi/SparkKinesisConsumerHudiProcessor.scala)

## Spark Submit 

SSH to master node and then run the spark submit command.
```
spark-submit \
--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
--conf "spark.sql.hive.convertMetastoreParquet=false" \
--conf "spk.dynamicAllocation.maxExecutors=10" \
--jars /usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar \
--packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.4.5,com.qubole.spark:spark-sql-kinesis_2.11:1.2.0_spark-2.4 \
--class kinesis.hudi.SparkKinesisConsumerHudiProcessor Spark-Structured-Streaming-Kinesis-Hudi-assembly-1.0.jar \
<bucket-name>  <stream-name> <region> <COW/MOR> <table_name>
	

```
## Spark Shell
Run the shell with command below and copy paste code from   [kinesis.hudi.SparkKinesisConsumerHudiProcessor](src/main/scala/kinesis/hudi/SparkKinesisConsumerHudiProcessor.scala). The code that needs to be copied is between  (Spark Shell ---Start ) and (Spark Shell ---End ). Also ensure that the you hard code the paremeters like s3_bucket, streamName, region ,tableType and hudiTableNamePrefix.  

```
spark-shell \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.hive.convertMetastoreParquet=false' \
--jars /usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar \
--packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.4.5,com.qubole.spark:spark-sql-kinesis_2.11:1.2.0_spark-2.4

```


# Use Case 3 - CDC event Published to S3 by DMS. S3 event triggered Lambda pushes file path to Kinesis. 
## Message Content pushed to the topic
The filePath here is the path to the file which got added to S3 by DMS. An S3 event gets published which is consumed by Lambda. The lambda then pushes the event below to the Kinesis stream which the file path of the file that got ingested. 
```
{
    "filePath": "s3://<bucket-name>/dms-full-load-path/salesdb/SALES_ORDER_DETAIL/20211118-100428844.parquet"
}
```
## Spark Scala Code
[kinesis.hudi.SparkKinesisFilePathConsumerHudiProcessor](src/main/scala/kinesis/hudi/SparkKinesisFilePathConsumerHudiProcessor.scala)

## Spark Submit 
    
    
```
spark-submit \
--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
--conf "spark.sql.hive.convertMetastoreParquet=false" \
--conf "spk.dynamicAllocation.maxExecutors=10" \
--jars /usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar \
--packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.4.5,com.qubole.spark:spark-sql-kinesis_2.11:1.2.0_spark-2.4 \
--class kinesis.hudi.SparkKinesisFilePathConsumerHudiProcessor Spark-Structured-Streaming-Kinesis-Hudi-assembly-1.0.jar \
<bucket-name>  <stream-name> <region> <COW/MOR> <table_name>
	

```

## Spark Shell

Run the shell with command below and copy paste code from   [kinesis.hudi.SparkKinesisFilePathConsumerHudiProcessor](src/main/scala/kinesis/hudi/SparkKinesisFilePathConsumerHudiProcessor.scala). The code that needs to be copied is between  (Spark Shell ---Start ) and (Spark Shell ---End ). Also ensure that the you hard code the paremeters like s3_bucket, streamName, region ,tableType and hudiTableNamePrefix.  

```

spark-shell \
--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
--conf 'spark.sql.hive.convertMetastoreParquet=false' \
--jars /usr/lib/hudi/hudi-spark-bundle.jar,/usr/lib/spark/external/lib/spark-avro.jar \
--packages org.apache.spark:spark-streaming-kinesis-asl_2.11:2.4.5,com.qubole.spark:spark-sql-kinesis_2.11:1.2.0_spark-2.4
```
