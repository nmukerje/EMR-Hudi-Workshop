package com.hudiConsumer

import com.amazonaws.regions.Regions
import com.amazonaws.services.cloudwatch.model.{Dimension, MetricDatum, PutMetricDataRequest, StandardUnit}
import com.amazonaws.services.cloudwatch.{AmazonCloudWatch, AmazonCloudWatchClientBuilder}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.hudi.DataSourceWriteOptions
import java.util.Calendar
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType

object SparkKafkaConsumerHudiProcessor_COW {

  //pushes stream metrics to Cloudwatch
  def pushCWMetric(client: AmazonCloudWatch, metric_value: Double ) = {

    val dimension = new Dimension()
      .withName("RECORDS_PROCESSED")
      .withValue("EVENTS_RECEIVED")

    val datum = new MetricDatum()
      .withMetricName("STREAMING_METRICS")
      .withUnit(StandardUnit.None)
      .withValue(metric_value)
      .withDimensions(dimension)

    val request = new PutMetricDataRequest()
      .withNamespace("STREAMING_METRICS")
      .withMetricData(datum)

    val response = client.putMetricData(request)

    SparkCustomLogger.log.info(response)

  }

  def main(args: Array[String]): Unit = {

    // The target S3 bucket
    val s3_bucket = args(0)
    // The kafka broker urls
    val brokers = args(1)

    // hive table and s3 table location
    val hudiTableName = "sales_order_detail_hudi_cow"
    val hudiTableRecordKey = "record_key"
    val hudiTablePartitionKey = "partition_key"
    val hudiTablePrecombineKey = "order_date"
    val hudiTablePath = s"s3://$s3_bucket/hudi/" + hudiTableName
    val hudiHiveTablePartitionKey = "year,month"

    // our kafka events topic
    val topics = Array("s3_event_stream")

    // Kafka configuration params
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "testgrp1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "security.protocol" -> "SSL"
    )

    val spark = SparkSession.builder().appName("SparkKafkaConsumerHudiProcessor_COW").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // init CloudWatch client
    val client = AmazonCloudWatchClientBuilder.standard
      .build

    // 2 min micro batches
    val ssc = new StreamingContext(sc, Seconds(120))

    // Build a DirectStream over the "events" kafka topic.
    val dstream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    // Write the data out to Parquet in S3
    dstream.foreachRDD { (rdd,time) =>
      // only if records are present
      if(rdd.count()>0)
      {
        // log batch time
        SparkCustomLogger.log.info("Time : "+time )

        // get offsets to save
        val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}")
          .mkString(",")
        SparkCustomLogger.log.info("Offset Ranges : "+offsetsRangesStr )

        // parse json events
        val json_df = spark.read.json(rdd.map( _.value))
        //val final_df=json_df.coalesce(1)//.dropDuplicates()
        json_df.show(10,false)

        val count_files = json_df.count()
        SparkCustomLogger.log.info("Files found in Batch : "+count_files )

        val filePaths=json_df.rdd.map(r => r(0)).collect()
        val filePaths_str=filePaths.map(_.toString)
        SparkCustomLogger.log.info(filePaths_str.mkString(","))
        var updated_df=spark.read.parquet(filePaths_str:_*).cache()
        val count_records = updated_df.count()

        //prepare the dataframe for Hudi updates
        updated_df=updated_df.drop("Op")
        updated_df=updated_df.select(updated_df.columns.map(x => col(x).as(x.toLowerCase)): _*)
        updated_df = updated_df.withColumn(hudiTableRecordKey, concat(col("order_id"), lit("#"), col("line_id")))
        updated_df = updated_df.withColumn("order_date", updated_df("order_date").cast(DateType))
        updated_df = updated_df.withColumn("year",year($"order_date").cast(StringType))
          .withColumn("month",month($"order_date").cast(StringType))

        updated_df = updated_df.withColumn(hudiTablePartitionKey,concat(lit("year="),$"year",lit("/month="),$"month"))

        updated_df.printSchema()
        updated_df.select("record_key","quantity").show()
        SparkCustomLogger.log.info("Records found in Batch : "+count_records )

        updated_df.write
          .format("org.apache.hudi")
          // COW Storage Type
          .option(DataSourceWriteOptions.STORAGE_TYPE_OPT_KEY, DataSourceWriteOptions.COW_STORAGE_TYPE_OPT_VAL)
          .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, hudiTableRecordKey)
          .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, hudiTablePartitionKey)
          .option(HoodieWriteConfig.TABLE_NAME, hudiTableName)
          .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
          .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, hudiTablePrecombineKey)
          .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true")
          .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY,hudiTableName)
          .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, hudiHiveTablePartitionKey)
          .option(DataSourceWriteOptions.HIVE_ASSUME_DATE_PARTITION_OPT_KEY, "false")
          .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getName)
          .mode(SaveMode.Append)
          .save(hudiTablePath)

        // save streaming metrics to Cloudwatch
        pushCWMetric(client,count_records)

        // commit offsets to Kafkaite
        dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetsRanges)

        // demonstrate Glue/Hive catalog tables
        val output = spark.sql("show databases").show()
        SparkCustomLogger.log.info(output)
        val output1 = spark.sql("show tables").show(5,false)
        SparkCustomLogger.log.info(output1)

      }
    }

    // Start the context
    ssc.start
    ssc.awaitTermination()

  }

}
