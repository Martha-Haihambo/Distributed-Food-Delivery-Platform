import ballerina/log;
import ballerina/os;
import ballerina/time;
import ballerinax/kafka;

final string kafkaBootstrap = os:getEnv("KAFKA_BOOTSTRAP") != "" ? os:getEnv("KAFKA_BOOTSTRAP") : "localhost:9092";

// ---- Producer: Order Service publishes orders.* and payments.requested ----

final kafka:ProducerConfiguration producerConfig = {
    clientId: "order-service-producer",
    acks: "all",
    retryCount: 3
};

final kafka:Producer orderProducer = check new (kafkaBootstrap, producerConfig);

// All order-lifecycle topics are keyed by orderId so every event for one order lands on
// the same partition and is processed in order.
function publishEvent(string topic, string key, json payload) returns error? {
    byte[] valueBytes = (check payload.toJsonString()).toBytes();
    check orderProducer->send({topic: topic, key: key.toBytes(), value: valueBytes});
    log:printInfo(string `published ${topic} key=${key}`);
}

function publishOrderCreated(Order o) returns error? {
    OrderCreatedEvent event = {
        orderId: o.id,
        customerId: o.customerId,
        restaurantId: o.restaurantId,
        items: o.items ?: [],
        totalAmount: o.totalAmount,
        createdAt: o.createdAt
    };
    check publishEvent("orders.created", o.id, event.toJson());
}

function publishPaymentRequested(string orderId, int customerId, decimal amount) returns error? {
    PaymentRequestedEvent event = {orderId, customerId, amount};
    check publishEvent("payments.requested", orderId, event.toJson());
}

function publishOrderPreparing(string orderId) returns error? {
    OrderPreparingEvent event = {orderId, startedAt: time:utcToString(time:utcNow())};
    check publishEvent("orders.preparing", orderId, event.toJson());
}

function publishOrderCancelled(string orderId, string reason) returns error? {
    OrderCancelledEvent event = {orderId, reason, cancelledAt: time:utcToString(time:utcNow())};
    check publishEvent("orders.cancelled", orderId, event.toJson());
}

// ---- Consumer: Order Service reacts to events produced by other services ----
// Consumer group "order-service-group" so multiple replicas of this service share the
// partitions instead of each getting a full copy.

final kafka:ConsumerConfiguration consumerConfig = {
    groupId: "order-service-group",
    topics: [
        "orders.confirmed",
        "orders.rejected",
        "payments.completed",
        "payments.failed",
        "delivery.assigned",
        "delivery.completed"
    ],
    offsetReset: "earliest",
    autoCommit: true
};

listener kafka:Listener orderEventsListener = check new (kafkaBootstrap, consumerConfig);

service on orderEventsListener {

    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string topic = rec.offset.partition.topic;
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
            error? handleResult = handleIncomingEvent(topic, payload);
            if handleResult is error {
                // Log and move on rather than crashing the consumer loop — a poison
                // message should not block the whole partition. In a production system
                // this would go to a dead-letter topic instead.
                log:printError(string `error handling event from ${topic}`, handleResult);
            }
        }
    }
}

function handleIncomingEvent(string topic, json payload) returns error? {
    match topic {
        "orders.confirmed" => {
            OrderConfirmedEvent event = check payload.cloneWithType(OrderConfirmedEvent);
            check updateOrderStatus(event.orderId, CONFIRMED);
            Order o = check getOrderById(event.orderId);
            check publishPaymentRequested(o.id, o.customerId, o.totalAmount);
        }
        "orders.rejected" => {
            OrderRejectedEvent event = check payload.cloneWithType(OrderRejectedEvent);
            check updateOrderStatus(event.orderId, CANCELLED);
            check publishOrderCancelled(event.orderId, event.reason);
        }
        "payments.completed" => {
            PaymentCompletedEvent event = check payload.cloneWithType(PaymentCompletedEvent);
            check updateOrderStatus(event.orderId, PREPARING);
            check publishOrderPreparing(event.orderId);
        }
        "payments.failed" => {
            PaymentFailedEvent event = check payload.cloneWithType(PaymentFailedEvent);
            check updateOrderStatus(event.orderId, CANCELLED);
            check publishOrderCancelled(event.orderId, string `payment failed: ${event.reason}`);
        }
        "delivery.assigned" => {
            DeliveryAssignedEvent event = check payload.cloneWithType(DeliveryAssignedEvent);
            check updateOrderStatus(event.orderId, OUT_FOR_DELIVERY);
        }
        "delivery.completed" => {
            DeliveryCompletedEvent event = check payload.cloneWithType(DeliveryCompletedEvent);
            check updateOrderStatus(event.orderId, DELIVERED);
        }
        _ => {
            log:printWarn(string `unhandled topic ${topic}`);
        }
    }
}
