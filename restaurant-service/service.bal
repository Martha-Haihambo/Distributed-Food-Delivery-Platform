import ballerina/http;
import ballerina/log;

listener http:Listener restaurantListener = new (8082);

service /restaurants on restaurantListener {

    // GET /restaurants/{id}
    resource function get [int id]() returns Restaurant|http:NotFound {
        Restaurant|error r = getRestaurant(id);
        if r is error {
            return <http:NotFound>{body: {message: string `restaurant ${id} not found`}};
        }
        return r;
    }

    // PATCH /restaurants/{id}/hours
    resource function patch [int id]/hours(HoursUpdate upd) returns http:Ok|http:InternalServerError {
        error? result = updateHours(id, upd);
        if result is error {
            log:printError("failed to update hours", result);
            return <http:InternalServerError>{body: {message: "could not update hours"}};
        }
        return <http:Ok>{body: {message: "hours updated"}};
    }

    // GET /restaurants/{id}/menu
    resource function get [int id]/menu() returns MenuItem[]|http:InternalServerError {
        MenuItem[]|error items = listMenu(id);
        if items is error {
            log:printError("failed to list menu", items);
            return <http:InternalServerError>{body: {message: "could not list menu"}};
        }
        return items;
    }

    // POST /restaurants/{id}/menu
    resource function post [int id]/menu(NewMenuItem newItem) returns MenuItem|http:InternalServerError {
        MenuItem|error created = addMenuItem(id, newItem);
        if created is error {
            log:printError("failed to add menu item", created);
            return <http:InternalServerError>{body: {message: "could not add menu item"}};
        }
        return created;
    }

    // PATCH /restaurants/{restaurantId}/menu/{menuItemId}/inventory
    resource function patch [int restaurantId]/menu/[int menuItemId]/inventory(InventoryUpdate upd)
            returns http:Ok|http:NotFound|http:InternalServerError {
        error? result = setInventory(menuItemId, upd.inStock);
        if result is error {
            return <http:NotFound>{body: {message: result.message()}};
        }
        return <http:Ok>{body: {message: "inventory updated"}};
    }

    // POST /restaurants/{id}/orders/{orderId}/ready
    // Staff-facing action: the kitchen marks an order ready for pickup, which publishes
    // orders.ready and hands the order off to the Delivery Service.
    resource function post [int id]/orders/[string orderId]/ready() returns http:Ok|http:InternalServerError {
        Restaurant|error r = getRestaurant(id);
        if r is error {
            return <http:InternalServerError>{body: {message: string `restaurant ${id} not found`}};
        }
        string pickupAddress = string `${r.name}, Windhoek`;
        error? result = publishOrderReady(orderId, id, pickupAddress);
        if result is error {
            log:printError("failed to publish orders.ready", result);
            return <http:InternalServerError>{body: {message: "could not mark order ready"}};
        }
        return <http:Ok>{body: {message: string `order ${orderId} marked ready`}};
    }
}
