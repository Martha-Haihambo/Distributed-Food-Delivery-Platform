import ballerina/os;
import ballerina/sql;
import ballerinax/postgresql;

final string dbHost = os:getEnv("DB_HOST") != "" ? os:getEnv("DB_HOST") : "localhost";
final int dbPort = check int:fromString(os:getEnv("DB_PORT") != "" ? os:getEnv("DB_PORT") : "5432");
final string dbName = os:getEnv("DB_NAME") != "" ? os:getEnv("DB_NAME") : "fooddelivery";
final string dbUser = os:getEnv("DB_USER") != "" ? os:getEnv("DB_USER") : "fooddelivery";
final string dbPassword = os:getEnv("DB_PASSWORD") != "" ? os:getEnv("DB_PASSWORD") : "fooddelivery";

final postgresql:Client dbClient = check new (
    host = dbHost,
    port = dbPort,
    database = dbName,
    username = dbUser,
    password = dbPassword,
    connectionPool = {maxOpenConnections: 10}
);

function createCustomer(NewCustomer nc) returns Customer|error {
    sql:ExecutionResult result = check dbClient->execute(`
        INSERT INTO customers (name, email, phone) VALUES (${nc.name}, ${nc.email}, ${nc?.phone})
    `);
    int|string? id = result.lastInsertId;
    int newId = id is int ? id : 0;
    return {id: newId, name: nc.name, email: nc.email, phone: nc?.phone};
}

function getCustomer(int id) returns Customer|error {
    record {|int id; string name; string email; string? phone;|} row = check dbClient->queryRow(`
        SELECT id, name, email, phone FROM customers WHERE id = ${id}
    `);
    return {id: row.id, name: row.name, email: row.email, phone: row.phone};
}

function addAddress(int customerId, NewAddress addr) returns Address|error {
    sql:ExecutionResult result = check dbClient->execute(`
        INSERT INTO addresses (customer_id, line1, city, is_default)
        VALUES (${customerId}, ${addr.line1}, ${addr.city}, ${addr.isDefault})
    `);
    int|string? id = result.lastInsertId;
    int newId = id is int ? id : 0;
    return {id: newId, customerId, line1: addr.line1, city: addr.city, isDefault: addr.isDefault};
}

function getOrderHistory(int customerId) returns OrderHistoryEntry[]|error {
    stream<record {|
        string order_id;
        int customer_id;
        string status;
        decimal total_amount;
        string updated_at;
    |}, sql:Error?> rows = dbClient->query(`
        SELECT order_id, customer_id, status, total_amount, updated_at::text AS updated_at
        FROM customer_order_history WHERE customer_id = ${customerId} ORDER BY updated_at DESC
    `);
    OrderHistoryEntry[] result = [];
    check from var r in rows
        do {
            result.push({
                orderId: r.order_id,
                customerId: r.customer_id,
                status: r.status,
                totalAmount: r.total_amount,
                updatedAt: r.updated_at
            });
        };
    return result;
}

// ---- Read-model maintenance (called from the Kafka consumer) ----

function upsertOrderHistoryOnCreate(string orderId, int customerId, decimal totalAmount) returns error? {
    _ = check dbClient->execute(`
        INSERT INTO customer_order_history (order_id, customer_id, status, total_amount, updated_at)
        VALUES (${orderId}, ${customerId}, 'CREATED', ${totalAmount}, now())
        ON CONFLICT (order_id) DO NOTHING
    `);
}

function updateOrderHistoryStatus(string orderId, string status) returns error? {
    _ = check dbClient->execute(`
        UPDATE customer_order_history SET status = ${status}, updated_at = now() WHERE order_id = ${orderId}
    `);
}
