public type Customer record {|
    int id;
    string name;
    string email;
    string? phone;
|};

public type NewCustomer record {|
    string name;
    string email;
    string phone?;
|};

public type Address record {|
    int id;
    int customerId;
    string line1;
    string city;
    boolean isDefault;
|};

public type NewAddress record {|
    string line1;
    string city;
    boolean isDefault = false;
|};

public type OrderHistoryEntry record {|
    string orderId;
    int customerId;
    string status;
    decimal totalAmount;
    string updatedAt;
|};

// ---- Kafka payload shapes this service listens to, to keep its order-history
// read-model up to date without ever querying Order Service's database directly ----
// These are deliberately OPEN records (`record { ... }`, no `|` pipes): each event on the
// wire carries more fields than this service needs (e.g. orders.created also has
// `items`), and an open record tolerates the extras instead of failing cloneWithType.

public type OrderCreatedEvent record {
    string orderId;
    int customerId;
    int restaurantId;
    decimal totalAmount;
    string createdAt;
};

public type OrderStatusEvent record {
    string orderId;
};
