FROM ballerina/ballerina:2201.10.2 AS build
WORKDIR /build
COPY . .
RUN bal build

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /build/target/bin/notification_service.jar ./notification_service.jar
EXPOSE 8086
ENTRYPOINT ["java", "-jar", "notification_service.jar"]
