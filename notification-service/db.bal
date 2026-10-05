import ballerina/os;
import ballerina/time;
import ballerina/uuid;
import ballerinax/mongodb;

final string mongoUri = os:getEnv("MONGO_URI") != "" ? os:getEnv("MONGO_URI") : "mongodb://localhost:27017/fooddelivery";

final mongodb:Client mongoClient = check new ({connection: mongoUri});

// A `->` action call cannot appear directly inside a module-level variable initializer
// in Ballerina ("action invocation as an expression not allowed here") — verified
// against a real `bal build`. Wrapping in a function body, then `check`-ing the
// function's own result at module level, is fine.
function initCollections() returns [mongodb:Database, mongodb:Collection]|error {
    mongodb:Database db = check mongoClient->getDatabase("fooddelivery");
    mongodb:Collection notifications = check db->getCollection("notifications");
    return [db, notifications];
}

final [mongodb:Database, mongodb:Collection] dbInit = check initCollections();
final mongodb:Database fdDatabase = dbInit[0];
final mongodb:Collection notificationsCollection = dbInit[1];

function recordNotification(string orderId, string recipientType, string channel, string message) returns error? {
    NotificationRecord n = {
        id: uuid:createType4AsString(),
        orderId,
        recipientType,
        channel,
        message,
        createdAt: time:utcToString(time:utcNow())
    };
    check notificationsCollection->insertOne(n);
}

function listNotificationsForOrder(string orderId) returns NotificationRecord[]|error {
    stream<NotificationRecord, error?> results = check notificationsCollection->find({orderId});
    NotificationRecord[] out = [];
    check from var n in results
        do {
            out.push(n);
        };
    return out;
}
