In this example, we will use Apache Hudi's [UserDefinedMetricsReporter](https://hudi.apache.org/docs/metrics.html#userdefinedmetricsreporter) to push Hudi metrics to Amazon CloudWatch. 

The CloudWatchMetricsReporter class extends the AbstractUserDefinedMetricsReporter class and pushed the metrics to Cloudwatch. You can change the implementation to say push only certain metrics or change the data types of the metrics e.g. report insertTime in seconds etc.

Here is an example of how the metrics will be structured in Cloudwatch:

Namespace : Hudi
Dimension ; <Hudi tablename>
Metric Key : commit.totalInsertRecordsWritten
Matric Value : 16292

## Step 1: Build CloudWatchMetricsReporter

We can build this class on EMR:

```
$> javac -cp /usr/lib/hudi/hudi-utilities-bundle.jar:/usr/lib/hudi/cli/lib/log4j-1.2.17.jar:/usr/lib/hudi/cli/lib/metrics-core-4.1.1.jar:/usr/share/aws/aws-java-sdk/aws-java-sdk-bundle-1.11.977.jar CloudWatchMetricsReporter.java
$> cp CloudWatchMetricsReporter.class custom/
$> jar -cvf cwmetricsreporter.jar custom/CloudWatchMetricsReporter.class
```

## Step 2: Add CloudWatchMetricsReporter to Hudi classpath

e.g. 

```
spark-submit --jars hdfs:///httpcore-4.4.11.jar,hdfs:///httpclient-4.5.9.jar,hdfs:///hudi-spark-bundle.jar,hdfs:///spark-avro.jar,hdfs:///cwmetricsreporter.jar \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
--conf "spark.sql.shuffle.partitions=500" \
--conf "spark.dynamicAllocation.initialExecutors=251" \
--conf "spark.dynamicAllocation.executorIdleTimeout=500" \
Hudi-Spark-Streaming-Ingestion.py
```

## Step 3: Set Hudi Config

Set the Hudi properties below to enable the metrics reporter:

```
hoodie.metrics.on=true
hoodie.metrics.reporter.class=custom.CloudWatchMetricsReporter
```

Once setup, you should be able to view metrics in Cloudwatch:

<img src="https://raw.githubusercontent.com/nmukerje/EMR-Hudi-Workshop/111d6f6bff6dfccb95343241fbfccace702d3555/cloudwatchmetricsreporter/Hudi_Streaming_Metrics_Dashboard.png" width="600"/>


