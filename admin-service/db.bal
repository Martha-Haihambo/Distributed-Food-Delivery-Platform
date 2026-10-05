import ballerina/os;
import ballerina/time;
import ballerinax/mongodb;

final string mongoUri = os:getEnv("MONGO_URI") != "" ? os:getEnv("MONGO_URI") : "mongodb://localhost:27017/fooddelivery";

final mongodb:Client mongoClient = check new ({connection: mongoUri});

// A `->` action call cannot appear directly inside a module-level variable initializer
// in Ballerina ("action invocation as an expression not allowed here") — verified
// against a real `bal build`. Wrapping in a function body, then `check`-ing the
// function's own result at module level, is fine.
function initCollections() returns [mongodb:Database, mongodb:Collection]|error {
    mongodb:Database db = check mongoClient->getDatabase("fooddelivery");
    mongodb:Collection projections = check db->getCollection("order_projections");
    return [db, projections];
}

final [mongodb:Database, mongodb:Collection] dbInit = check initCollections();
final mongodb:Database fdDatabase = dbInit[0];
final mongodb:Collection projectionsCollection = dbInit[1];

function upsertProjectionField(string orderId, map<json> fields) returns error? {
    _ = check projectionsCollection->updateOne(
        {orderId},
        {set: fields},
        {upsert: true}
    );
}

function onOrderCreated(OrderCreatedEvent event) returns error? {
    check upsertProjectionField(event.orderId, {
        orderId: event.orderId,
        restaurantId: event.restaurantId,
        customerId: event.customerId,
        totalAmount: event.totalAmount,
        createdAt: event.createdAt
    });
}

function findProjectionsByRestaurant(int restaurantId) returns OrderProjection[]|error {
    stream<OrderProjection, error?> results = check projectionsCollection->find({restaurantId});
    OrderProjection[] out = [];
    check from var p in results
        do {
            out.push(p);
        };
    return out;
}

function findAllAssignedProjections() returns OrderProjection[]|error {
    stream<OrderProjection, error?> results = check projectionsCollection->find({assignedAt: {"$ne": null}});
    OrderProjection[] out = [];
    check from var p in results
        do {
            out.push(p);
        };
    return out;
}

// ---- Reporting: computed in application code over the projection documents rather ----
// ---- than a database aggregation pipeline, to keep the logic easy to defend/explain ----

function minutesBetween(string startIso, string endIso) returns decimal|error {
    time:Utc startUtc = check time:utcFromString(startIso);
    time:Utc endUtc = check time:utcFromString(endIso);
    decimal seconds = time:utcDiffSeconds(endUtc, startUtc);
    return seconds / 60;
}

function computeRestaurantStats(int restaurantId) returns RestaurantStats|error {
    OrderProjection[] projections = check findProjectionsByRestaurant(restaurantId);

    int totalOrders = projections.length();
    int deliveredOrders = 0;
    int cancelledOrders = 0;
    decimal revenue = 0;
    decimal prepMinutesSum = 0;
    int prepSamples = 0;

    foreach OrderProjection p in projections {
        if p.deliveredAt is string {
            deliveredOrders += 1;
            revenue += p.totalAmount ?: 0d;
        }
        if p.cancelledAt is string || p.rejectedAt is string {
            cancelledOrders += 1;
        }
        string? preparingAt = p.preparingAt;
        string? readyAt = p.readyAt;
        if preparingAt is string && readyAt is string {
            decimal|error mins = minutesBetween(preparingAt, readyAt);
            if mins is decimal {
                prepMinutesSum += mins;
                prepSamples += 1;
            }
        }
    }

    decimal? avgPrep = prepSamples > 0 ? prepMinutesSum / <decimal>prepSamples : ();

    return {
        restaurantId,
        totalOrders,
        deliveredOrders,
        cancelledOrders,
        revenue,
        avgPrepMinutes: avgPrep
    };
}

function computeDeliveryPerformance() returns DeliveryPerformanceReport|error {
    OrderProjection[] projections = check findAllAssignedProjections();

    int totalAssigned = 0;
    int totalCompleted = 0;
    decimal overallMinutesSum = 0;
    int overallSamples = 0;

    map<[int, decimal, int]> perDriver = {}; // driverId -> [completedCount, minutesSum, samples]

    foreach OrderProjection p in projections {
        string? driverId = p.driverId;
        string? assignedAt = p.assignedAt;
        if driverId is () || assignedAt is () {
            continue;
        }
        totalAssigned += 1;

        [int, decimal, int] current = perDriver[driverId] ?: [0, 0d, 0];

        string? deliveredAt = p.deliveredAt;
        if deliveredAt is string {
            totalCompleted += 1;
            decimal|error mins = minutesBetween(assignedAt, deliveredAt);
            if mins is decimal {
                overallMinutesSum += mins;
                overallSamples += 1;
                current = [current[0] + 1, current[1] + mins, current[2] + 1];
            } else {
                current = [current[0] + 1, current[1], current[2]];
            }
        }
        perDriver[driverId] = current;
    }

    DriverPerformance[] byDriver = [];
    foreach [string, [int, decimal, int]] entry in perDriver.entries() {
        string driverId = entry[0];
        [int, decimal, int] stats = entry[1];
        decimal? avg = stats[2] > 0 ? stats[1] / <decimal>stats[2] : ();
        byDriver.push({driverId, completedDeliveries: stats[0], avgDeliveryMinutes: avg});
    }

    decimal? overallAvg = overallSamples > 0 ? overallMinutesSum / <decimal>overallSamples : ();

    return {
        totalDeliveriesAssigned: totalAssigned,
        totalDeliveriesCompleted: totalCompleted,
        avgDeliveryMinutes: overallAvg,
        byDriver
    };
}
