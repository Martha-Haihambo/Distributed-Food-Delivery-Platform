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

// The UNIQUE constraint on transactions.order_id (see infra/postgres-init/001-schema.sql)
// is what makes this idempotent under Kafka's at-least-once redelivery: a second attempt
// to insert for the same order fails, and findTransactionByOrderId lets the caller reuse
// the already-decided result instead of charging twice.
function findTransactionByOrderId(string orderId) returns Transaction|error? {
    Transaction|sql:Error result = dbClient->queryRow(`
        SELECT id, order_id AS "orderId", customer_id AS "customerId", amount, status,
               failure_reason AS "failureReason", created_at::text AS "createdAt"
        FROM transactions WHERE order_id = ${orderId}
    `);
    if result is sql:NoRowsError {
        return ();
    }
    if result is sql:Error {
        return result;
    }
    return result;
}

function recordTransaction(string orderId, int customerId, decimal amount, string status, string? failureReason)
        returns Transaction|error {
    sql:ExecutionResult execResult = check dbClient->execute(`
        INSERT INTO transactions (order_id, customer_id, amount, status, failure_reason)
        VALUES (${orderId}, ${customerId}, ${amount}, ${status}, ${failureReason})
        ON CONFLICT (order_id) DO NOTHING
    `);
    if execResult.affectedRowCount == 0 {
        // Someone beat us to it (duplicate delivery) — return the existing record.
        Transaction? existing = check findTransactionByOrderId(orderId);
        if existing is Transaction {
            return existing;
        }
        return error(string `transaction for order ${orderId} could not be recorded or found`);
    }
    Transaction? created = check findTransactionByOrderId(orderId);
    if created is Transaction {
        return created;
    }
    return error(string `transaction for order ${orderId} was inserted but could not be re-read`);
}

function getTransaction(string orderId) returns Transaction|error {
    Transaction? t = check findTransactionByOrderId(orderId);
    if t is Transaction {
        return t;
    }
    return error(string `no transaction found for order ${orderId}`);
}
