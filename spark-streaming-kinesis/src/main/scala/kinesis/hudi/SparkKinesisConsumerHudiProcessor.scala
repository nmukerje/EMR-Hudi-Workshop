package kinesis.hudi

import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.hudi.DataSourceWriteOptions
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.KinesisInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kinesis.KinesisInitialPositions

object SparkKinesisConsumerHudiProcessor {

  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
          .builder
          .appName("SparkHudi")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.default.parallelism", 9)
          .config("spark.sql.shuffle.partitions", 9)
          .enableHiveSupport()
          .getOrCreate()
    
    import spark.implicits._

    val s3_bucket=args(0)
    val streamName=args(1)
    val region=args(2)
    val tableType=args(3)
    var hudiTableName = "sales_order_detail_hudi_cow"
    var dsWriteOptionType=DataSourceWriteOptions.COW_STORAGE_TYPE_OPT_VAL
    if(tableType.equals("COW")){
       hudiTableName = "sales_order_detail_hudi_cow"
       dsWriteOptionType=DataSourceWriteOptions.COW_STORAGE_TYPE_OPT_VAL
    }else if (tableType.equals("MOR")){
      hudiTableName = "sales_order_detail_hudi_mor"
      dsWriteOptionType=DataSourceWriteOptions.MOR_STORAGE_TYPE_OPT_VAL
    }

    val dataPath=s"s3://$s3_bucket/dms-full-load-path/salesdb/SALES_ORDER_DETAIL/LOAD*"
    val hudiTableRecordKey = "record_key"
    val hudiTablePartitionKey = "partition_key"
    val hudiTablePrecombineKey = "order_date"
    val hudiTablePath = s"s3://$s3_bucket/hudi/" + hudiTableName
    val hudiHiveTablePartitionKey = "year,month"
    val checkpoint_path=s"s3://$s3_bucket/kinesis-checkpoint/"
    val endpointUrl=s"https://kinesis.$region.amazonaws.com"

    
   
  
    val streamingInputDF = (spark
                    .readStream .format("kinesis") 
                    .option("streamName", "hudi-stream") 
                    .option("startingposition", "TRIM_HORIZON")
                    .option("endpointUrl", endpointUrl)
                    .load())


    val schema= StructType(Array(StructField("filePath",StringType,true)))
    

    val jsonDF=(streamingInputDF.selectExpr("CAST(data AS STRING)").as[(String)]
                .withColumn("jsonData",from_json(col("data"),schema))
                .select(col("jsonData.filePath")))
    
    jsonDF.printSchema()
    val query = (jsonDF.writeStream.foreachBatch{ (batchDF: DataFrame, _: Long) => {
            print(batchDF)
            batchDF.collect().foreach( s=> { 
                print(s)
                var parDF=spark.read.format("parquet").load(s.getString(0));
                parDF=parDF.drop("Op")
                parDF=parDF.select(parDF.columns.map(x => col(x).as(x.toLowerCase)): _*)
                parDF = parDF.withColumn(hudiTableRecordKey, concat(col("order_id"), lit("#"), col("line_id")))
                parDF = parDF.withColumn("order_date", parDF("order_date").cast(DateType))
                parDF = parDF.withColumn("year",year($"order_date").cast(StringType)).withColumn("month",month($"order_date").cast(StringType))
                parDF = parDF.withColumn(hudiTablePartitionKey,concat(lit("year="),$"year",lit("/month="),$"month"))
                parDF.printSchema()
                parDF.select("month","record_key","quantity").show()
                parDF.write.format("org.apache.hudi")
                      .option("hoodie.datasource.write.table.type", dsWriteOptionType)
                      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, hudiTableRecordKey)
                      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, hudiTablePartitionKey)
                      .option(HoodieWriteConfig.TABLE_NAME, hudiTableName)
                      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
                      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, hudiTablePrecombineKey)
                      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true")
                      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY,hudiTableName)
                      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, hudiHiveTablePartitionKey)
                      .option("hoodie.datasource.hive_sync.assume_date_partitioning", "false")
                      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getName)
                      .mode("append")
                      .save(hudiTablePath);
            })
    }}.option("checkpointLocation", checkpoint_path).start())
   

    query.awaitTermination()

  }

}
