FROM openjdk:8
WORKDIR /app


COPY target/kinesis-producer-1.0.0.jar /app/app.jar
COPY src/main/config /app/config
USER root
ENTRYPOINT ["java", "-jar" ,"/app/app.jar", "--spring.config.location=/app/config/application.properties"]
