FROM ballerina/ballerina:2201.10.2 AS build
WORKDIR /build
COPY . .
RUN bal build

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /build/target/bin/delivery_service.jar ./delivery_service.jar
EXPOSE 8085
ENTRYPOINT ["java", "-jar", "delivery_service.jar"]
