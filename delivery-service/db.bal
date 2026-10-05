import ballerina/os;
import ballerina/time;
import ballerina/uuid;
import ballerinax/mongodb;

// Mongo suits this service better than SQL: driver location updates are frequent,
// semi-structured, and don't need relational joins the way orders/payments do.
final string mongoUri = os:getEnv("MONGO_URI") != "" ? os:getEnv("MONGO_URI") : "mongodb://localhost:27017/fooddelivery";

final mongodb:Client mongoClient = check new ({connection: mongoUri});

// A `->` action call (getDatabase/getCollection) cannot appear directly inside a
// module-level variable initializer in Ballerina ("action invocation as an expression
// not allowed here") — verified against a real `bal build`. Wrapping the calls in a
// function body, then `check`-ing the function's own result at module level, is fine.
function initCollections() returns [mongodb:Database, mongodb:Collection, mongodb:Collection]|error {
    mongodb:Database db = check mongoClient->getDatabase("fooddelivery");
    mongodb:Collection drivers = check db->getCollection("drivers");
    mongodb:Collection deliveries = check db->getCollection("deliveries");
    return [db, drivers, deliveries];
}

final [mongodb:Database, mongodb:Collection, mongodb:Collection] dbInit = check initCollections();
final mongodb:Database fdDatabase = dbInit[0];
final mongodb:Collection driversCollection = dbInit[1];
final mongodb:Collection deliveriesCollection = dbInit[2];

// OPEN records (no `|` pipes): read back from MongoDB, which adds its own `_id` field
// neither type declares — open records tolerate that instead of failing to deserialize.
type DriverDoc record {
    string id;
    string name;
    string status;
    decimal? lat;
    decimal? lng;
};

type DeliveryDoc record {
    string orderId;
    string driverId;
    string status;
    string pickupAddress;
    string assignedAt;
    string? deliveredAt;
    LocationHistoryEntry[] locationHistory;
};

function createDriver(string name) returns Driver|error {
    string id = uuid:createType4AsString();
    DriverDoc doc = {id, name, status: "AVAILABLE", lat: (), lng: ()};
    check driversCollection->insertOne(doc);
    return {id, name, status: "AVAILABLE", currentLocation: ()};
}

function setDriverAvailability(string id, string status) returns error? {
    mongodb:UpdateResult result = check driversCollection->updateOne({id}, {set: {status}});
    if result.matchedCount == 0 {
        return error(string `driver ${id} not found`);
    }
}

function findAvailableDriver() returns DriverDoc?|error {
    stream<DriverDoc, error?> results = check driversCollection->find({status: "AVAILABLE"});
    record {|DriverDoc value;|}|error? next = results.next();
    check results.close();
    if next is error {
        return next;
    }
    if next is record {|DriverDoc value;|} {
        return next.value;
    }
    return ();
}

function updateDriverLocation(string driverId, decimal lat, decimal lng) returns error? {
    mongodb:UpdateResult result = check driversCollection->updateOne({id: driverId}, {set: {lat, lng}});
    if result.matchedCount == 0 {
        return error(string `driver ${driverId} not found`);
    }
}

function createDelivery(string orderId, string driverId, string pickupAddress) returns Delivery|error {
    string nowStr = time:utcToString(time:utcNow());
    DeliveryDoc doc = {
        orderId,
        driverId,
        status: "ASSIGNED",
        pickupAddress,
        assignedAt: nowStr,
        deliveredAt: (),
        locationHistory: []
    };
    check deliveriesCollection->insertOne(doc);
    return {
        orderId,
        driverId,
        status: "ASSIGNED",
        pickupAddress,
        assignedAt: nowStr,
        deliveredAt: (),
        locationHistory: []
    };
}

function getDelivery(string orderId) returns Delivery|error {
    // findOne returns `targetType|Error?` — nil when nothing matches — so `check` alone
    // narrows to `DeliveryDoc?`, not `DeliveryDoc`. Verified against the connector's real
    // `collection.bal` source; must handle the nil case explicitly instead of assigning
    // straight into a non-optional variable.
    DeliveryDoc? doc = check deliveriesCollection->findOne({orderId});
    if doc is () {
        return error(string `delivery for order ${orderId} not found`);
    }
    return {
        orderId: doc.orderId,
        driverId: doc.driverId,
        status: doc.status,
        pickupAddress: doc.pickupAddress,
        assignedAt: doc.assignedAt,
        deliveredAt: doc.deliveredAt,
        locationHistory: doc.locationHistory
    };
}

// Finds the active (not yet delivered) delivery for a driver, if any — used to append
// location pings to the right order's history.
function findActiveDeliveryForDriver(string driverId) returns DeliveryDoc?|error {
    stream<DeliveryDoc, error?> results = check deliveriesCollection->find({driverId, status: "ASSIGNED"});
    record {|DeliveryDoc value;|}|error? next = results.next();
    check results.close();
    if next is error {
        return next;
    }
    if next is record {|DeliveryDoc value;|} {
        return next.value;
    }
    return ();
}

function appendLocationToDelivery(string orderId, decimal lat, decimal lng) returns error? {
    LocationHistoryEntry entry = {lat, lng, timestamp: time:utcToString(time:utcNow())};
    _ = check deliveriesCollection->updateOne({orderId}, {push: {locationHistory: entry}});
}

function markDelivered(string orderId) returns error? {
    string nowStr = time:utcToString(time:utcNow());
    mongodb:UpdateResult result = check deliveriesCollection->updateOne(
        {orderId},
        {set: {status: "DELIVERED", deliveredAt: nowStr}}
    );
    if result.matchedCount == 0 {
        return error(string `delivery for order ${orderId} not found`);
    }
}
