import ballerina/http;
import ballerina/log;

listener http:Listener adminListener = new (8087);

service /admin/reports on adminListener {

    // GET /admin/reports/restaurants/{id}/stats
    resource function get restaurants/[int id]/stats() returns RestaurantStats|http:InternalServerError {
        RestaurantStats|error stats = computeRestaurantStats(id);
        if stats is error {
            log:printError("failed to compute restaurant stats", stats);
            return <http:InternalServerError>{body: {message: "could not compute restaurant stats"}};
        }
        return stats;
    }

    // GET /admin/reports/delivery-performance
    resource function get delivery\-performance() returns DeliveryPerformanceReport|http:InternalServerError {
        DeliveryPerformanceReport|error report = computeDeliveryPerformance();
        if report is error {
            log:printError("failed to compute delivery performance", report);
            return <http:InternalServerError>{body: {message: "could not compute delivery performance"}};
        }
        return report;
    }
}
