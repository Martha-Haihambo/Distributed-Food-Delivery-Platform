// OPEN (no `|` pipes): read back from MongoDB, which adds its own `_id` field that this
// type doesn't declare — an open record tolerates that instead of failing to deserialize.
public type NotificationRecord record {
    string id;
    string orderId;
    string recipientType; // CUSTOMER | RESTAURANT | DRIVER
    string channel;       // EMAIL | SMS | PUSH (simulated — logged, not actually sent)
    string message;
    string createdAt;
};

// ---- Minimal shapes we need out of each event — only the fields used to build a message.
// All OPEN records: every event on the wire carries more fields than this service needs,
// and an open record tolerates the extras instead of failing cloneWithType. ----

public type OrderCreatedEvent record {
    string orderId;
    int restaurantId;
};

public type OrderConfirmedEvent record {
    string orderId;
    int estimatedPrepMinutes?;
};

public type OrderRejectedEvent record {
    string orderId;
    string reason;
};

public type OrderPreparingEvent record {
    string orderId;
};

public type OrderReadyEvent record {
    string orderId;
};

public type OrderCancelledEvent record {
    string orderId;
    string reason;
};

public type PaymentCompletedEvent record {
    string orderId;
    decimal amount;
};

public type PaymentFailedEvent record {
    string orderId;
    string reason;
};

public type DeliveryAssignedEvent record {
    string orderId;
    string driverId;
};

public type DeliveryCompletedEvent record {
    string orderId;
    string driverId;
};
