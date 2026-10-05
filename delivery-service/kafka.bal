import ballerina/log;
import ballerina/os;
import ballerina/time;
import ballerinax/kafka;

final string kafkaBootstrap = os:getEnv("KAFKA_BOOTSTRAP") != "" ? os:getEnv("KAFKA_BOOTSTRAP") : "localhost:9092";

final kafka:ProducerConfiguration producerConfig = {
    clientId: "delivery-service-producer",
    acks: "all",
    retryCount: 3
};

final kafka:Producer deliveryProducer = check new (kafkaBootstrap, producerConfig);

function publishEvent(string topic, string key, json payload) returns error? {
    byte[] valueBytes = (check payload.toJsonString()).toBytes();
    check deliveryProducer->send({topic: topic, key: key.toBytes(), value: valueBytes});
    log:printInfo(string `published ${topic} key=${key}`);
}

function publishDeliveryAssigned(string orderId, string driverId) returns error? {
    DeliveryAssignedEvent event = {orderId, driverId, assignedAt: time:utcToString(time:utcNow())};
    check publishEvent("delivery.assigned", orderId, event.toJson());
}

// delivery.location_updated is keyed by driverId (not orderId) — it's a high-frequency,
// order-independent stream, unlike the rest of the order-lifecycle topics.
function publishLocationUpdated(string driverId, string? orderId, decimal lat, decimal lng) returns error? {
    DeliveryLocationUpdatedEvent event = {driverId, orderId, lat, lng, timestamp: time:utcToString(time:utcNow())};
    check publishEvent("delivery.location_updated", driverId, event.toJson());
}

function publishDeliveryCompleted(string orderId, string driverId) returns error? {
    DeliveryCompletedEvent event = {orderId, driverId, deliveredAt: time:utcToString(time:utcNow())};
    check publishEvent("delivery.completed", orderId, event.toJson());
}

// ---- Consumer: assign a driver as soon as the restaurant marks an order ready ----

final kafka:ConsumerConfiguration consumerConfig = {
    groupId: "delivery-service-group",
    topics: ["orders.ready"],
    offsetReset: "earliest",
    autoCommit: true
};

listener kafka:Listener deliveryEventsListener = check new (kafkaBootstrap, consumerConfig);

service on deliveryEventsListener {

    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string|error payloadStr = string:fromBytes(rec.value);
            if payloadStr is error {
                log:printError("failed to decode kafka message", payloadStr);
                continue;
            }
            json|error payload = payloadStr.fromJsonString();
            if payload is error {
                log:printError("failed to parse kafka message as json", payload);
                continue;
            }
            error? result = handleOrderReady(payload);
            if result is error {
                log:printError("error handling orders.ready", result);
            }
        }
    }
}

function handleOrderReady(json payload) returns error? {
    OrderReadyEvent event = check payload.cloneWithType(OrderReadyEvent);

    DriverDoc? driver = check findAvailableDriver();
    if driver is () {
        // No bonus route-optimization here — simplest possible dispatch policy: first
        // available driver. Worth calling out as a known limitation in the defence.
        log:printWarn(string `no available driver for order ${event.orderId}; leaving unassigned`);
        return;
    }

    check setDriverAvailability(driver.id, "BUSY");
    _ = check createDelivery(event.orderId, driver.id, event.pickupAddress);
    check publishDeliveryAssigned(event.orderId, driver.id);
}
