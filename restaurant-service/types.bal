public type Restaurant record {|
    int id;
    string name;
    string openingTime;
    string closingTime;
    boolean isOpen;
|};

public type MenuItem record {|
    int id;
    int restaurantId;
    string name;
    decimal price;
    boolean inStock;
|};

public type NewMenuItem record {|
    string name;
    decimal price;
    boolean inStock = true;
|};

public type InventoryUpdate record {|
    boolean inStock;
|};

public type HoursUpdate record {|
    string openingTime?;
    string closingTime?;
    boolean isOpen?;
|};

// ---- Kafka payload shapes shared with Order Service (docs/architecture.md section 3) ----

public type OrderItemEvent record {|
    int menuItemId;
    string name;
    int quantity;
    decimal unitPrice;
|};

public type OrderCreatedEvent record {|
    string orderId;
    int customerId;
    int restaurantId;
    OrderItemEvent[] items;
    decimal totalAmount;
    string createdAt;
|};

public type OrderConfirmedEvent record {|
    string orderId;
    int restaurantId;
    int estimatedPrepMinutes;
|};

public type OrderRejectedEvent record {|
    string orderId;
    string reason;
|};

public type OrderReadyEvent record {|
    string orderId;
    int restaurantId;
    string pickupAddress;
|};
