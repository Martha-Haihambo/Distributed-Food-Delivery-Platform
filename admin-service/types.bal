// Admin Service builds its own read-store by consuming the event stream (CQRS-style, as
// suggested in docs/architecture.md section 5) rather than calling every other service's
// REST API to assemble a report. One projection document per order, progressively filled
// in as lifecycle events arrive.
//
// This is an OPEN record (no `|` pipes): MongoDB documents come back with a driver-added
// `_id` field this type doesn't declare, and an open record tolerates that instead of
// failing to deserialize.
public type OrderProjection record {
    string orderId;
    int? restaurantId = ();
    int? customerId = ();
    decimal? totalAmount = ();
    string? createdAt = ();
    string? confirmedAt = ();
    string? rejectedAt = ();
    string? preparingAt = ();
    string? readyAt = ();
    string? assignedAt = ();
    string? driverId = ();
    string? deliveredAt = ();
    string? cancelledAt = ();
};

public type RestaurantStats record {|
    int restaurantId;
    int totalOrders;
    int deliveredOrders;
    int cancelledOrders;
    decimal revenue;
    decimal? avgPrepMinutes;
|};

public type DriverPerformance record {|
    string driverId;
    int completedDeliveries;
    decimal? avgDeliveryMinutes;
|};

public type DeliveryPerformanceReport record {|
    int totalDeliveriesAssigned;
    int totalDeliveriesCompleted;
    decimal? avgDeliveryMinutes;
    DriverPerformance[] byDriver;
|};

// ---- Kafka payload shapes (only the fields this service needs) ----
// OPEN records throughout: this service only cares about a subset of each event's
// fields (e.g. orders.created also carries `items`, which we ignore), and an open
// record tolerates the fields it doesn't declare instead of failing cloneWithType.

public type OrderCreatedEvent record {
    string orderId;
    int customerId;
    int restaurantId;
    decimal totalAmount;
    string createdAt;
};

public type OrderIdOnlyEvent record {
    string orderId;
};

public type DeliveryAssignedEvent record {
    string orderId;
    string driverId;
    string assignedAt;
};

public type DeliveryCompletedEvent record {
    string orderId;
    string driverId;
    string deliveredAt;
};
