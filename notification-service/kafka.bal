import ballerina/log;
import ballerina/os;
import ballerinax/kafka;

final string kafkaBootstrap = os:getEnv("KAFKA_BOOTSTRAP") != "" ? os:getEnv("KAFKA_BOOTSTRAP") : "localhost:9092";

// Notification Service fans out across nearly the whole event stream — it is the one
// service that legitimately needs a copy of almost every topic. delivery.location_updated
// is deliberately excluded: it fires too often per order to turn into user-facing alerts.
final kafka:ConsumerConfiguration consumerConfig = {
    groupId: "notification-service-group",
    topics: [
        "orders.created",
        "orders.confirmed",
        "orders.rejected",
        "orders.preparing",
        "orders.ready",
        "orders.cancelled",
        "payments.completed",
        "payments.failed",
        "delivery.assigned",
        "delivery.completed"
    ],
    offsetReset: "earliest",
    autoCommit: true
};

listener kafka:Listener notificationEventsListener = check new (kafkaBootstrap, consumerConfig);

service on notificationEventsListener {

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
            error? result = dispatch(topic, payload);
            if result is error {
                log:printError(string `error handling event from ${topic}`, result);
            }
        }
    }
}

// Simulated multi-channel delivery: log + persist. CUSTOMER -> EMAIL, RESTAURANT -> SMS,
// DRIVER -> PUSH is an arbitrary but consistent mapping for the demo.
function notify(string orderId, string recipientType, string message) returns error? {
    string channel = channelForRecipient(recipientType);
    log:printInfo(string `[${channel} -> ${recipientType}] order ${orderId}: ${message}`);
    check recordNotification(orderId, recipientType, channel, message);
}

// Plain if-else rather than a `match` expression: this compiler rejects
// `T x = match y { pattern => value, ... };` as a parse error even though the
// `match` *statement* form (`match y { pattern => { ...statements... } }`) used
// elsewhere in this file works fine — verified against a real `bal build`.
function channelForRecipient(string recipientType) returns string {
    if recipientType == "CUSTOMER" {
        return "EMAIL";
    } else if recipientType == "RESTAURANT" {
        return "SMS";
    } else if recipientType == "DRIVER" {
        return "PUSH";
    }
    return "EMAIL";
}

function dispatch(string topic, json payload) returns error? {
    match topic {
        "orders.created" => {
            OrderCreatedEvent event = check payload.cloneWithType(OrderCreatedEvent);
            check notify(event.orderId, "RESTAURANT", "New order received — please confirm or reject.");
        }
        "orders.confirmed" => {
            OrderConfirmedEvent event = check payload.cloneWithType(OrderConfirmedEvent);
            check notify(event.orderId, "CUSTOMER", "Your order has been confirmed by the restaurant.");
        }
        "orders.rejected" => {
            OrderRejectedEvent event = check payload.cloneWithType(OrderRejectedEvent);
            check notify(event.orderId, "CUSTOMER", string `Your order was rejected: ${event.reason}`);
        }
        "orders.preparing" => {
            OrderPreparingEvent event = check payload.cloneWithType(OrderPreparingEvent);
            check notify(event.orderId, "CUSTOMER", "Payment received — your order is being prepared.");
        }
        "orders.ready" => {
            OrderReadyEvent event = check payload.cloneWithType(OrderReadyEvent);
            check notify(event.orderId, "CUSTOMER", "Your order is ready and awaiting driver pickup.");
        }
        "orders.cancelled" => {
            OrderCancelledEvent event = check payload.cloneWithType(OrderCancelledEvent);
            check notify(event.orderId, "CUSTOMER", string `Your order was cancelled: ${event.reason}`);
        }
        "payments.completed" => {
            PaymentCompletedEvent event = check payload.cloneWithType(PaymentCompletedEvent);
            check notify(event.orderId, "CUSTOMER", string `Payment of N$${event.amount} received.`);
        }
        "payments.failed" => {
            PaymentFailedEvent event = check payload.cloneWithType(PaymentFailedEvent);
            check notify(event.orderId, "CUSTOMER", string `Payment failed: ${event.reason}`);
        }
        "delivery.assigned" => {
            DeliveryAssignedEvent event = check payload.cloneWithType(DeliveryAssignedEvent);
            check notify(event.orderId, "CUSTOMER", "A driver has been assigned to your order.");
            check notify(event.orderId, "DRIVER", string `You have a new delivery for order ${event.orderId}.`);
        }
        "delivery.completed" => {
            DeliveryCompletedEvent event = check payload.cloneWithType(DeliveryCompletedEvent);
            check notify(event.orderId, "CUSTOMER", "Your order has been delivered. Enjoy!");
        }
        _ => {
            log:printWarn(string `unhandled topic ${topic}`);
        }
    }
}
