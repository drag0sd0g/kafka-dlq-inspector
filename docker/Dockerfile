FROM eclipse-temurin:21-jre
WORKDIR /app
COPY build/libs/kafka-dlq-inspector-0.1.0-all.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java","-jar","/app/app.jar"]
