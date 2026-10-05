// Domain types for the Order Service.
// This service owns the order lifecycle end-to-end; every other service learns order
// status by consuming these events or calling GET /orders/{id} — never by reading the DB.

public type OrderItemInput record {|
    int menuItemId;
    string name;
    int quantity;
    decimal unitPrice;
|};

public type CreateOrderRequest record {|
    int customerId;
    int restaurantId;
    OrderItemInput[] items;
    string deliveryAddress;
|};

public type OrderItem record {|
    int menuItemId;
    string name;
    int quantity;
    decimal unitPrice;
|};

public type Order record {|
    string id;
    int customerId;
    int restaurantId;
    string status;
    decimal totalAmount;
    string deliveryAddress;
    OrderItem[] items?;
    string createdAt;
    string updatedAt;
|};

// ---- Kafka event payload shapes (see docs/architecture.md section 3) ----

public type OrderCreatedEvent record {|
    string orderId;
    int customerId;
    int restaurantId;
    OrderItem[] items;
    decimal totalAmount;
    string createdAt;
|};

public type OrderConfirmedEvent record {|
    string orderId;
    int restaurantId;
    int estimatedPrepMinutes?;
|};

public type OrderRejectedEvent record {|
    string orderId;
    string reason;
|};

public type PaymentRequestedEvent record {|
    string orderId;
    int customerId;
    decimal amount;
|};

public type PaymentCompletedEvent record {|
    string orderId;
    string transactionId;
    decimal amount;
    string paidAt;
|};

public type PaymentFailedEvent record {|
    string orderId;
    string reason;
|};

public type OrderPreparingEvent record {|
    string orderId;
    string startedAt;
|};

public type OrderReadyEvent record {|
    string orderId;
    int restaurantId;
    string pickupAddress;
|};

public type DeliveryAssignedEvent record {|
    string orderId;
    string driverId;
    string assignedAt;
|};

public type DeliveryCompletedEvent record {|
    string orderId;
    string driverId;
    string deliveredAt;
|};

public type OrderCancelledEvent record {|
    string orderId;
    string reason;
    string cancelledAt;
|};

// Valid order states — used to guard illegal transitions server-side.
public enum OrderStatus {
    CREATED,
    CONFIRMED,
    PREPARING,
    READY,
    OUT_FOR_DELIVERY,
    DELIVERED,
    CANCELLED
}
