FROM ballerina/ballerina:2201.10.2 AS build
WORKDIR /build
COPY . .
RUN bal build

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /build/target/bin/restaurant_service.jar ./restaurant_service.jar
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "restaurant_service.jar"]
