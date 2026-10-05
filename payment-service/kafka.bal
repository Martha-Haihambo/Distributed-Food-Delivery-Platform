import ballerina/log;
import ballerina/os;
import ballerina/random;
import ballerina/time;
import ballerinax/kafka;

final string kafkaBootstrap = os:getEnv("KAFKA_BOOTSTRAP") != "" ? os:getEnv("KAFKA_BOOTSTRAP") : "localhost:9092";

final kafka:ProducerConfiguration producerConfig = {
    clientId: "payment-service-producer",
    acks: "all",
    retryCount: 3
};

final kafka:Producer paymentProducer = check new (kafkaBootstrap, producerConfig);

function publishEvent(string topic, string key, json payload) returns error? {
    byte[] valueBytes = (check payload.toJsonString()).toBytes();
    check paymentProducer->send({topic: topic, key: key.toBytes(), value: valueBytes});
    log:printInfo(string `published ${topic} key=${key}`);
}

// ---- Consumer: simulate charging a card when Order Service asks us to ----

final kafka:ConsumerConfiguration consumerConfig = {
    groupId: "payment-service-group",
    topics: ["payments.requested"],
    offsetReset: "earliest",
    autoCommit: true
};

listener kafka:Listener paymentEventsListener = check new (kafkaBootstrap, consumerConfig);

service on paymentEventsListener {

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
            error? result = handlePaymentRequested(payload);
            if result is error {
                log:printError("error handling payments.requested", result);
            }
        }
    }
}

function handlePaymentRequested(json payload) returns error? {
    PaymentRequestedEvent event = check payload.cloneWithType(PaymentRequestedEvent);

    // Idempotency guard: if we've already decided this order (duplicate delivery from
    // Kafka's at-least-once semantics), just re-emit the same outcome instead of
    // charging again.
    Transaction? existing = check findTransactionByOrderId(event.orderId);
    if existing is Transaction {
        check republishOutcome(existing);
        return;
    }

    // Simulated processing: ~90% success rate, deterministic enough to demo failure
    // handling on demand without a real payment gateway.
    int roll = check random:createIntInRange(1, 101);
    boolean success = roll <= 90;

    if success {
        Transaction t = check recordTransaction(event.orderId, event.customerId, event.amount, "COMPLETED", ());
        PaymentCompletedEvent completedEvent = {
            orderId: event.orderId,
            transactionId: t.id.toString(),
            amount: event.amount,
            paidAt: time:utcToString(time:utcNow())
        };
        check publishEvent("payments.completed", event.orderId, completedEvent.toJson());
    } else {
        string reason = "card declined (simulated)";
        _ = check recordTransaction(event.orderId, event.customerId, event.amount, "FAILED", reason);
        PaymentFailedEvent failedEvent = {orderId: event.orderId, reason};
        check publishEvent("payments.failed", event.orderId, failedEvent.toJson());
    }
}

function republishOutcome(Transaction t) returns error? {
    if t.status == "COMPLETED" {
        PaymentCompletedEvent completedEvent = {
            orderId: t.orderId,
            transactionId: t.id.toString(),
            amount: t.amount,
            paidAt: t.createdAt
        };
        check publishEvent("payments.completed", t.orderId, completedEvent.toJson());
    } else {
        PaymentFailedEvent failedEvent = {orderId: t.orderId, reason: t.failureReason ?: "unknown"};
        check publishEvent("payments.failed", t.orderId, failedEvent.toJson());
    }
}
