import ballerina/log;
import ballerina/os;
import ballerinax/kafka;

final string kafkaBootstrap = os:getEnv("KAFKA_BOOTSTRAP") != "" ? os:getEnv("KAFKA_BOOTSTRAP") : "localhost:9092";

final kafka:ConsumerConfiguration consumerConfig = {
    groupId: "admin-service-group",
    topics: [
        "orders.created",
        "orders.confirmed",
        "orders.rejected",
        "orders.preparing",
        "orders.ready",
        "orders.cancelled",
        "delivery.assigned",
        "delivery.completed"
    ],
    offsetReset: "earliest",
    autoCommit: true
};

listener kafka:Listener adminEventsListener = check new (kafkaBootstrap, consumerConfig);

service on adminEventsListener {

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
            error? result = project(topic, payload);
            if result is error {
                log:printError(string `error projecting event from ${topic}`, result);
            }
        }
    }
}

function project(string topic, json payload) returns error? {
    match topic {
        "orders.created" => {
            OrderCreatedEvent event = check payload.cloneWithType(OrderCreatedEvent);
            check onOrderCreated(event);
        }
        "orders.confirmed" => {
            OrderIdOnlyEvent event = check payload.cloneWithType(OrderIdOnlyEvent);
            check upsertProjectionField(event.orderId, {confirmedAt: currentIso()});
        }
        "orders.rejected" => {
            OrderIdOnlyEvent event = check payload.cloneWithType(OrderIdOnlyEvent);
            check upsertProjectionField(event.orderId, {rejectedAt: currentIso()});
        }
        "orders.preparing" => {
            OrderIdOnlyEvent event = check payload.cloneWithType(OrderIdOnlyEvent);
            check upsertProjectionField(event.orderId, {preparingAt: currentIso()});
        }
        "orders.ready" => {
            OrderIdOnlyEvent event = check payload.cloneWithType(OrderIdOnlyEvent);
            check upsertProjectionField(event.orderId, {readyAt: currentIso()});
        }
        "orders.cancelled" => {
            OrderIdOnlyEvent event = check payload.cloneWithType(OrderIdOnlyEvent);
            check upsertProjectionField(event.orderId, {cancelledAt: currentIso()});
        }
        "delivery.assigned" => {
            DeliveryAssignedEvent event = check payload.cloneWithType(DeliveryAssignedEvent);
            check upsertProjectionField(event.orderId, {assignedAt: event.assignedAt, driverId: event.driverId});
        }
        "delivery.completed" => {
            DeliveryCompletedEvent event = check payload.cloneWithType(DeliveryCompletedEvent);
            check upsertProjectionField(event.orderId, {deliveredAt: event.deliveredAt});
        }
        _ => {
            log:printWarn(string `unhandled topic ${topic}`);
        }
    }
}
