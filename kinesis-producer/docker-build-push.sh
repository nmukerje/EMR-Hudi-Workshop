 docker build . -f Dockerfile -t kinesis-producers/kinesis-producer
 docker tag kinesis-producers/kinesis-producer:latest 799223504601.dkr.ecr.ap-south-1.amazonaws.com/kinesis-consumers/kinesis-producer:latest
 docker push 799223504601.dkr.ecr.ap-south-1.amazonaws.com/kinesis-consumers/kinesis-producer:latest
