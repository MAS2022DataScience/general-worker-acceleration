FROM openjdk:20-jdk-slim-buster

ENV DATAPLATFORM_IP=localhost

WORKDIR /app
COPY ./target/general-worker-acceleration-0.0.1-SNAPSHOT.jar /app

EXPOSE 8080

CMD ["java", "-jar", "general-worker-acceleration-0.0.1-SNAPSHOT.jar"]