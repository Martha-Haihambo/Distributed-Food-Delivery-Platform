FROM ballerina/ballerina:2201.10.2 AS build
WORKDIR /build
COPY . .
RUN bal build

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /build/target/bin/admin_service.jar ./admin_service.jar
EXPOSE 8087
ENTRYPOINT ["java", "-jar", "admin_service.jar"]
