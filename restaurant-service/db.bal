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

function getRestaurant(int id) returns Restaurant|error {
    record {|
        int id;
        string name;
        string opening_time;
        string closing_time;
        boolean is_open;
    |} row = check dbClient->queryRow(`
        SELECT id, name, opening_time::text AS opening_time, closing_time::text AS closing_time, is_open
        FROM restaurants WHERE id = ${id}
    `);
    return {
        id: row.id,
        name: row.name,
        openingTime: row.opening_time,
        closingTime: row.closing_time,
        isOpen: row.is_open
    };
}

function updateHours(int id, HoursUpdate upd) returns error? {
    if upd.openingTime is string {
        _ = check dbClient->execute(`UPDATE restaurants SET opening_time = ${<string>upd.openingTime}::time WHERE id = ${id}`);
    }
    if upd.closingTime is string {
        _ = check dbClient->execute(`UPDATE restaurants SET closing_time = ${<string>upd.closingTime}::time WHERE id = ${id}`);
    }
    if upd.isOpen is boolean {
        _ = check dbClient->execute(`UPDATE restaurants SET is_open = ${<boolean>upd.isOpen} WHERE id = ${id}`);
    }
}

function listMenu(int restaurantId) returns MenuItem[]|error {
    stream<record {|int id; int restaurant_id; string name; decimal price; boolean in_stock;|}, sql:Error?> rows =
        dbClient->query(`SELECT id, restaurant_id, name, price, in_stock FROM menu_items WHERE restaurant_id = ${restaurantId}`);
    MenuItem[] result = [];
    check from var r in rows
        do {
            result.push({id: r.id, restaurantId: r.restaurant_id, name: r.name, price: r.price, inStock: r.in_stock});
        };
    return result;
}

function addMenuItem(int restaurantId, NewMenuItem item) returns MenuItem|error {
    sql:ExecutionResult result = check dbClient->execute(`
        INSERT INTO menu_items (restaurant_id, name, price, in_stock)
        VALUES (${restaurantId}, ${item.name}, ${item.price}, ${item.inStock})
    `);
    int|string? generatedId = result.lastInsertId;
    int newId = generatedId is int ? generatedId : 0;
    return {id: newId, restaurantId, name: item.name, price: item.price, inStock: item.inStock};
}

function setInventory(int menuItemId, boolean inStock) returns error? {
    sql:ExecutionResult result = check dbClient->execute(`UPDATE menu_items SET in_stock = ${inStock} WHERE id = ${menuItemId}`);
    if result.affectedRowCount == 0 {
        return error(string `menu item ${menuItemId} not found`);
    }
}

// Checks the two conditions that decide accept/reject for an incoming order: the
// restaurant is currently open, and every ordered item is in stock.
function canFulfil(int restaurantId, OrderItemEvent[] items) returns boolean|error {
    Restaurant r = check getRestaurant(restaurantId);
    if !r.isOpen {
        return false;
    }
    foreach OrderItemEvent it in items {
        record {|boolean in_stock;|} stockRow = check dbClient->queryRow(
            `SELECT in_stock FROM menu_items WHERE id = ${it.menuItemId} AND restaurant_id = ${restaurantId}`
        );
        if !stockRow.in_stock {
            return false;
        }
    }
    return true;
}
