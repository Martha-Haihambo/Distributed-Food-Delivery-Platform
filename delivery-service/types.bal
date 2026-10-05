public type Location record {|
    decimal lat;
    decimal lng;
|};

public type Driver record {|
    string id;
    string name;
    string status; // AVAILABLE | BUSY
    Location? currentLocation;
|};

public type NewDriver record {|
    string name;
|};

public type AvailabilityUpdate record {|
    string status; // AVAILABLE | BUSY
|};

public type LocationUpdate record {|
    decimal lat;
    decimal lng;
|};

public type LocationHistoryEntry record {|
    decimal lat;
    decimal lng;
    string timestamp;
|};

public type Delivery record {|
    string orderId;
    string driverId;
    string status; // ASSIGNED | DELIVERED
    string pickupAddress;
    string assignedAt;
    string? deliveredAt;
    LocationHistoryEntry[] locationHistory;
|};

// ---- Kafka payload shapes ----

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

public type DeliveryLocationUpdatedEvent record {|
    string driverId;
    string orderId?;
    decimal lat;
    decimal lng;
    string timestamp;
|};

public type DeliveryCompletedEvent record {|
    string orderId;
    string driverId;
    string deliveredAt;
|};
