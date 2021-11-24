
The code in this project creates dummy data with randomization and pushes to Kineis Stream. 

## Prerequisite 

1. Create KDS . Update  streamName and region in application.properties. 
2. The template of the messaage could be pushed from either a path on S3 or from the local file system. 
3. To pull the template from S3 , create S3 bucket where the template file will be placed. Create template file and upload that to a path. Update bucketName and templatePath property in application.properties. 
4. To pull the template from local file system where the code runs, keep bucketName as empty ("") in application.properties. Update templatePath in application.properties.

## How to run ?
1. Import the project as maven project in the IDE. 
2. Run the project as java application with main class as com.aksh.kinesis.producer.KinesisProducerMain 
3. The code will generate randomized messages according to the template and start pushing messages to Kinesis. 

```
KPL Push: {"tradeId":"211124204181756","symbol":"GOOGL","quantity":"39","price":"39","timestamp":1637766663,"description":"Traded on Wed Nov 24 20:41:03 IST 2021","traderName":"GOOGL trader","traderFirm":"GOOGL firm"}
KPL Push: {"tradeId":"211124204126307","symbol":"IBM","quantity":"99","price":"99","timestamp":1637766663,"description":"Traded on Wed Nov 24 20:41:03 IST 2021","traderName":"IBM trader","traderFirm":"IBM firm"}
KPL Push: {"tradeId":"211124204123699","symbol":"IBM","quantity":"69","price":"69","timestamp":1637766663,"description":"Traded on Wed Nov 24 20:41:03 IST 2021","traderName":"IBM trader","traderFirm":"IBM firm"}


```

## How to control rate of ingestion
1. Update intervalMs property in application.properies. 

## How to use KPL and aggregation ?
1. Update publishType=kpl
2. Update aggregationEnabled=true
