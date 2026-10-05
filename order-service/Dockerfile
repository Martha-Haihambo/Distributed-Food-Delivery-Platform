# Multi-stage build: compile with the Ballerina toolchain, run on a slim JRE.
FROM ballerina/ballerina:2201.10.2 AS build
WORKDIR /build
COPY . .
RUN bal build

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /build/target/bin/order_service.jar ./order_service.jar
EXPOSE 8083
ENTRYPOINT ["java", "-jar", "order_service.jar"]
