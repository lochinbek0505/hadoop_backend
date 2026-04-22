FROM eclipse-temurin:17-jre-alpine

WORKDIR /app
RUN addgroup -S spring && adduser -S spring -G spring
USER spring:spring

COPY target/hadoop-sim-backend-1.0.0.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java","-Xms128m","-Xmx384m","-XX:+UseSerialGC","-jar","/app/app.jar"]