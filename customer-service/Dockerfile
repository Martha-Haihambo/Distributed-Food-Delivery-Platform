FROM ballerina/ballerina:2201.10.2 AS build
WORKDIR /build
COPY . .
RUN bal build

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /build/target/bin/customer_service.jar ./customer_service.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "customer_service.jar"]
