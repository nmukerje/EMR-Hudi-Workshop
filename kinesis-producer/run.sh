
PROPERTIES_FILE=file:/Users/rawaaksh/code/aws-samples/kinesis-consumers/kcl-enhanced-consumer/src/main/config/$1
java -jar target/kcl-enhanced-consumer-1.0.0.jar --spring.config.location=$PROPERTIES_FILE
