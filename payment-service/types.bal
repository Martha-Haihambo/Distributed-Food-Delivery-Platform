public type Transaction record {|
    int id;
    string orderId;
    int customerId;
    decimal amount;
    string status; // COMPLETED | FAILED
    string? failureReason;
    string createdAt;
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
