import ballerina/os;
import ballerina/sql;
import ballerinax/postgresql;

// Read connection details from environment variables (set in docker-compose.yml).
// Falls back to localhost defaults so `bal run` also works against a locally-started
// Postgres for quick iteration outside Docker.
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

// ---- Persistence helpers ----

function insertOrder(Order o) returns error? {
    _ = check dbClient->execute(`
        INSERT INTO orders (id, customer_id, restaurant_id, status, total_amount, delivery_address, created_at, updated_at)
        VALUES (${o.id}, ${o.customerId}, ${o.restaurantId}, ${o.status}, ${o.totalAmount}, ${o.deliveryAddress}, now(), now())
    `);
    OrderItem[] items = o.items ?: [];
    foreach OrderItem it in items {
        _ = check dbClient->execute(`
            INSERT INTO order_items (order_id, menu_item_id, name, quantity, unit_price)
            VALUES (${o.id}, ${it.menuItemId}, ${it.name}, ${it.quantity}, ${it.unitPrice})
        `);
    }
}

function updateOrderStatus(string orderId, string status) returns error? {
    sql:ExecutionResult result = check dbClient->execute(`
        UPDATE orders SET status = ${status}, updated_at = now() WHERE id = ${orderId}
    `);
    if result.affectedRowCount == 0 {
        return error(string `order ${orderId} not found while setting status ${status}`);
    }
}

function getOrderById(string orderId) returns Order|error {
    record {|
        string id;
        int customer_id;
        int restaurant_id;
        string status;
        decimal total_amount;
        string delivery_address;
        string created_at;
        string updated_at;
    |} row = check dbClient->queryRow(`
        SELECT id, customer_id, restaurant_id, status, total_amount, delivery_address,
               created_at::text AS created_at, updated_at::text AS updated_at
        FROM orders WHERE id = ${orderId}
    `);

    stream<record {|int menuItemId; string name; int quantity; decimal unitPrice;|}, sql:Error?> itemStream =
        dbClient->query(`
            SELECT menu_item_id AS "menuItemId", name, quantity, unit_price AS "unitPrice"
            FROM order_items WHERE order_id = ${orderId}
        `);
    OrderItem[] items = [];
    check from var item in itemStream
        do {
            items.push({menuItemId: item.menuItemId, name: item.name, quantity: item.quantity, unitPrice: item.unitPrice});
        };

    return {
        id: row.id,
        customerId: row.customer_id,
        restaurantId: row.restaurant_id,
        status: row.status,
        totalAmount: row.total_amount,
        deliveryAddress: row.delivery_address,
        items: items,
        createdAt: row.created_at,
        updatedAt: row.updated_at
    };
}

function listOrders(int? customerId, int? restaurantId, string? status) returns Order[]|error {
    sql:ParameterizedQuery baseQuery = `SELECT id, customer_id, restaurant_id, status, total_amount, delivery_address,
               created_at::text AS created_at, updated_at::text AS updated_at FROM orders WHERE 1=1`;
    sql:ParameterizedQuery[] filters = [baseQuery];
    if customerId is int {
        filters.push(` AND customer_id = ${customerId}`);
    }
    if restaurantId is int {
        filters.push(` AND restaurant_id = ${restaurantId}`);
    }
    if status is string {
        filters.push(` AND status = ${status}`);
    }
    filters.push(` ORDER BY created_at DESC`);
    sql:ParameterizedQuery finalQuery = sql:queryConcat(...filters);

    stream<record {|
        string id;
        int customer_id;
        int restaurant_id;
        string status;
        decimal total_amount;
        string delivery_address;
        string created_at;
        string updated_at;
    |}, sql:Error?> rows = dbClient->query(finalQuery);

    Order[] result = [];
    check from var r in rows
        do {
            result.push({
                id: r.id,
                customerId: r.customer_id,
                restaurantId: r.restaurant_id,
                status: r.status,
                totalAmount: r.total_amount,
                deliveryAddress: r.delivery_address,
                createdAt: r.created_at,
                updatedAt: r.updated_at
            });
        };
    return result;
}
