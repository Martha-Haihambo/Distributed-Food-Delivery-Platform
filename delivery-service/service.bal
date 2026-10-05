import ballerina/http;
import ballerina/log;

listener http:Listener deliveryListener = new (8085);

service / on deliveryListener {

    // POST /drivers
    resource function post drivers(NewDriver newDriver) returns Driver|http:InternalServerError {
        Driver|error d = createDriver(newDriver.name);
        if d is error {
            log:printError("failed to create driver", d);
            return <http:InternalServerError>{body: {message: "could not create driver"}};
        }
        return d;
    }

    // PATCH /drivers/{id}/availability
    resource function patch drivers/[string id]/availability(AvailabilityUpdate upd)
            returns http:Ok|http:NotFound {
        error? result = setDriverAvailability(id, upd.status);
        if result is error {
            return <http:NotFound>{body: {message: result.message()}};
        }
        return <http:Ok>{body: {message: "availability updated"}};
    }

    // PATCH /drivers/{id}/location — driver app pushes live coordinates.
    // Publishes delivery.location_updated (bonus: driver-location-simulation feed for a
    // map overlay) and, if the driver currently has an active delivery, appends the ping
    // to that delivery's location history.
    resource function patch drivers/[string id]/location(LocationUpdate loc)
            returns http:Ok|http:InternalServerError {
        error? updateResult = updateDriverLocation(id, loc.lat, loc.lng);
        if updateResult is error {
            log:printError("failed to update driver location", updateResult);
            return <http:InternalServerError>{body: {message: "could not update location"}};
        }

        string? activeOrderId = ();
        DeliveryDoc?|error activeDelivery = findActiveDeliveryForDriver(id);
        if activeDelivery is DeliveryDoc {
            activeOrderId = activeDelivery.orderId;
            error? appendResult = appendLocationToDelivery(activeDelivery.orderId, loc.lat, loc.lng);
            if appendResult is error {
                log:printError("failed to append location history", appendResult);
            }
        }

        error? publishResult = publishLocationUpdated(id, activeOrderId, loc.lat, loc.lng);
        if publishResult is error {
            log:printError("failed to publish delivery.location_updated", publishResult);
        }
        return <http:Ok>{body: {message: "location updated"}};
    }

    // GET /deliveries/{orderId}
    resource function get deliveries/[string orderId]() returns Delivery|http:NotFound {
        Delivery|error d = getDelivery(orderId);
        if d is error {
            return <http:NotFound>{body: {message: string `no delivery found for order ${orderId}`}};
        }
        return d;
    }

    // POST /deliveries/{orderId}/complete — driver confirms drop-off.
    resource function post deliveries/[string orderId]/complete() returns http:Ok|http:InternalServerError {
        Delivery|error d = getDelivery(orderId);
        if d is error {
            return <http:InternalServerError>{body: {message: d.message()}};
        }

        error? markResult = markDelivered(orderId);
        if markResult is error {
            log:printError("failed to mark delivery complete", markResult);
            return <http:InternalServerError>{body: {message: "could not complete delivery"}};
        }

        error? availResult = setDriverAvailability(d.driverId, "AVAILABLE");
        if availResult is error {
            log:printError("failed to free up driver", availResult);
        }

        error? publishResult = publishDeliveryCompleted(orderId, d.driverId);
        if publishResult is error {
            log:printError("failed to publish delivery.completed", publishResult);
        }

        return <http:Ok>{body: {message: string `delivery for order ${orderId} completed`}};
    }
}
