FROM eclipse-temurin:17-jre-alpine

WORKDIR /app
RUN addgroup -S spring && adduser -S spring -G spring
USER spring:spring

COPY target/hadoop_back_end-0.0.1-SNAPSHOT.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java","-Xms128m","-Xmx384m","-XX:+UseSerialGC","-jar","/app/app.jar"]