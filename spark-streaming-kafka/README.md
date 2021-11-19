
# Spark Command
## Message Content pushed to the topic
The filePath here is the path to the file which got added to S3 by DMS. An S3 event gets published which is consumed by Lambda. The lambda then pushes the event below to the Spart topic which the file path of the file that got ingested. 
```
{
    "filePath": "s3://<bucket-name>/dms-full-load-path/salesdb/SALES_ORDER_DETAIL/20211118-100428844.parquet"
}
```

## Spark Submit

spark-submit --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" --conf "spark.sql.hive.convertMetastoreParquet=false" --conf "spark.dynamicAllocation.maxExecutors=10" --jars hdfs:///httpclient-4.5.9.jar,hdfs:///hudi-spark-bundle.jar,hdfs:///spark-avro.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,org.apache.spark:spark-streaming-kafka-0-10_2.11:2.4.5 --class kafka.hudi.SparkKafkaConsumerHudiProcessor Spark-Structured-Streaming-Hudi-assembly-1.0.jar <bucket-name> <kafka-bootstrap> <topic> <COW/MOR>

## Spark Shell
spark-shell --jars hdfs:///hudi-spark-bundle.jar,hdfs:///spark-avro.jar,hdfs:///httpclient-4.5.9.jar --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf 'spark.sql.hive.convertMetastoreParquet=false' --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,org.apache.spark:spark-streaming-kafka-0-10_2.11:2.4.5


