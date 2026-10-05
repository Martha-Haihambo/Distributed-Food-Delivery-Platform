FROM ballerina/ballerina:2201.10.2 AS build
WORKDIR /build
COPY . .
RUN bal build

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /build/target/bin/payment_service.jar ./payment_service.jar
EXPOSE 8084
ENTRYPOINT ["java", "-jar", "payment_service.jar"]
