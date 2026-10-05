import ballerina/log;
import ballerina/os;
import ballerinax/kafka;

final string kafkaBootstrap = os:getEnv("KAFKA_BOOTSTRAP") != "" ? os:getEnv("KAFKA_BOOTSTRAP") : "localhost:9092";

final kafka:ProducerConfiguration producerConfig = {
    clientId: "restaurant-service-producer",
    acks: "all",
    retryCount: 3
};

final kafka:Producer restaurantProducer = check new (kafkaBootstrap, producerConfig);

function publishEvent(string topic, string key, json payload) returns error? {
    byte[] valueBytes = (check payload.toJsonString()).toBytes();
    check restaurantProducer->send({topic: topic, key: key.toBytes(), value: valueBytes});
    log:printInfo(string `published ${topic} key=${key}`);
}

// Default prep-time estimate quoted back to the customer/Order Service on acceptance.
const int DEFAULT_PREP_MINUTES = 20;

function publishOrderConfirmed(string orderId, int restaurantId) returns error? {
    OrderConfirmedEvent event = {orderId, restaurantId, estimatedPrepMinutes: DEFAULT_PREP_MINUTES};
    check publishEvent("orders.confirmed", orderId, event.toJson());
}

function publishOrderRejected(string orderId, string reason) returns error? {
    OrderRejectedEvent event = {orderId, reason};
    check publishEvent("orders.rejected", orderId, event.toJson());
}

function publishOrderReady(string orderId, int restaurantId, string pickupAddress) returns error? {
    OrderReadyEvent event = {orderId, restaurantId, pickupAddress};
    check publishEvent("orders.ready", orderId, event.toJson());
}

// ---- Consumer: decide accept/reject on orders.created ----

final kafka:ConsumerConfiguration consumerConfig = {
    groupId: "restaurant-service-group",
    topics: ["orders.created"],
    offsetReset: "earliest",
    autoCommit: true
};

listener kafka:Listener restaurantEventsListener = check new (kafkaBootstrap, consumerConfig);

service on restaurantEventsListener {

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
            error? result = handleOrderCreated(payload);
            if result is error {
                log:printError("error handling orders.created", result);
            }
        }
    }
}

function handleOrderCreated(json payload) returns error? {
    OrderCreatedEvent event = check payload.cloneWithType(OrderCreatedEvent);

    boolean|error fulfilable = canFulfil(event.restaurantId, event.items);
    if fulfilable is error {
        log:printError(string `could not evaluate fulfilment for order ${event.orderId}`, fulfilable);
        check publishOrderRejected(event.orderId, "restaurant or menu item not found");
        return;
    }

    if fulfilable {
        check publishOrderConfirmed(event.orderId, event.restaurantId);
        // In this simulation the "kitchen" is fire-and-forget: real prep-time tracking
        // (a timer, or a staff-facing "mark ready" endpoint) would live here. For the
        // assignment we expose POST /restaurants/{id}/orders/{orderId}/ready so a staff
        // client (or a test script) can trigger the next step explicitly — see service.bal.
    } else {
        check publishOrderRejected(event.orderId, "restaurant closed or item out of stock");
    }
}
