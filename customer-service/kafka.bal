import ballerina/log;
import ballerina/os;
import ballerinax/kafka;

final string kafkaBootstrap = os:getEnv("KAFKA_BOOTSTRAP") != "" ? os:getEnv("KAFKA_BOOTSTRAP") : "localhost:9092";

// Customer Service never queries Order Service's database — it keeps its own
// order-history read-model up to date purely by consuming events, which is why this
// service subscribes to nearly the whole order lifecycle even though it never writes
// order state itself.
final kafka:ConsumerConfiguration consumerConfig = {
    groupId: "customer-service-group",
    topics: [
        "orders.created",
        "orders.confirmed",
        "orders.preparing",
        "orders.ready",
        "delivery.assigned",
        "delivery.completed",
        "orders.cancelled"
    ],
    offsetReset: "earliest",
    autoCommit: true
};

listener kafka:Listener customerEventsListener = check new (kafkaBootstrap, consumerConfig);

service on customerEventsListener {

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
            error? result = handleEvent(topic, payload);
            if result is error {
                log:printError(string `error handling event from ${topic}`, result);
            }
        }
    }
}

function handleEvent(string topic, json payload) returns error? {
    if topic == "orders.created" {
        OrderCreatedEvent event = check payload.cloneWithType(OrderCreatedEvent);
        check upsertOrderHistoryOnCreate(event.orderId, event.customerId, event.totalAmount);
        return;
    }

    // Every other topic here just moves the status forward; orderId is all we need
    // since the row already exists from orders.created.
    string status = statusForTopic(topic);
    if status == "" {
        log:printWarn(string `unhandled topic ${topic}`);
        return;
    }

    OrderStatusEvent event = check payload.cloneWithType(OrderStatusEvent);
    check updateOrderHistoryStatus(event.orderId, status);
}

// Plain if-else rather than a `match` expression: this compiler rejects
// `T x = match y { pattern => value, ... };` as a parse error even though the
// `match` *statement* form (`match y { pattern => { ...statements... } }`) used
// elsewhere in this file works fine — verified against a real `bal build`.
function statusForTopic(string topic) returns string {
    if topic == "orders.confirmed" {
        return "CONFIRMED";
    } else if topic == "orders.preparing" {
        return "PREPARING";
    } else if topic == "orders.ready" {
        return "READY";
    } else if topic == "delivery.assigned" {
        return "OUT_FOR_DELIVERY";
    } else if topic == "delivery.completed" {
        return "DELIVERED";
    } else if topic == "orders.cancelled" {
        return "CANCELLED";
    }
    return "";
}
