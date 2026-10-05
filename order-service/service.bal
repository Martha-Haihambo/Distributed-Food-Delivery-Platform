import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerina/uuid;

listener http:Listener orderListener = new (8083);

service /orders on orderListener {

    // POST /orders — create a new order. Always starts life as CREATED and emits
    // orders.created; the Restaurant Service decides accept/reject asynchronously.
    resource function post .(CreateOrderRequest req) returns Order|http:BadRequest|http:InternalServerError {
        if req.items.length() == 0 {
            return <http:BadRequest>{body: {message: "order must contain at least one item"}};
        }

        decimal total = 0;
        OrderItem[] items = [];
        foreach OrderItemInput it in req.items {
            if it.quantity <= 0 {
                return <http:BadRequest>{body: {message: string `quantity for ${it.name} must be positive`}};
            }
            total += it.unitPrice * <decimal>it.quantity;
            items.push({menuItemId: it.menuItemId, name: it.name, quantity: it.quantity, unitPrice: it.unitPrice});
        }

        string orderId = uuid:createType4AsString();
        string nowStr = time:utcToString(time:utcNow());
        Order o = {
            id: orderId,
            customerId: req.customerId,
            restaurantId: req.restaurantId,
            status: CREATED,
            totalAmount: total,
            deliveryAddress: req.deliveryAddress,
            items: items,
            createdAt: nowStr,
            updatedAt: nowStr
        };

        error? insertResult = insertOrder(o);
        if insertResult is error {
            log:printError("failed to persist order", insertResult);
            return <http:InternalServerError>{body: {message: "could not create order"}};
        }

        error? publishResult = publishOrderCreated(o);
        if publishResult is error {
            // The order is already durably persisted; a publish failure here is a gap
            // worth flagging in the defence (a production system would use an outbox
            // table + relay instead of "persist then publish" to guarantee delivery).
            log:printError("order persisted but failed to publish orders.created", publishResult);
        }

        return o;
    }

    // GET /orders/{id}
    resource function get [string id]() returns Order|http:NotFound|http:InternalServerError {
        Order|error o = getOrderById(id);
        if o is error {
            return <http:NotFound>{body: {message: string `order ${id} not found`}};
        }
        return o;
    }

    // GET /orders?customerId=&restaurantId=&status=
    resource function get .(int? customerId, int? restaurantId, string? status) returns Order[]|http:InternalServerError {
        Order[]|error result = listOrders(customerId, restaurantId, status);
        if result is error {
            log:printError("failed to list orders", result);
            return <http:InternalServerError>{body: {message: "could not list orders"}};
        }
        return result;
    }

    // PATCH /orders/{id}/cancel — customer-initiated cancel, only while the kitchen
    // hasn't started preparing yet.
    resource function patch [string id]/cancel() returns Order|http:Conflict|http:NotFound|http:InternalServerError {
        Order|error existing = getOrderById(id);
        if existing is error {
            return <http:NotFound>{body: {message: string `order ${id} not found`}};
        }
        if existing.status != CREATED && existing.status != CONFIRMED {
            return <http:Conflict>{
                body: {message: string `order ${id} cannot be cancelled once it is ${existing.status}`}
            };
        }

        error? updateResult = updateOrderStatus(id, CANCELLED);
        if updateResult is error {
            log:printError("failed to cancel order", updateResult);
            return <http:InternalServerError>{body: {message: "could not cancel order"}};
        }

        error? publishResult = publishOrderCancelled(id, "cancelled by customer");
        if publishResult is error {
            log:printError("order cancelled but failed to publish orders.cancelled", publishResult);
        }

        Order|error updated = getOrderById(id);
        if updated is error {
            return <http:InternalServerError>{body: {message: "cancelled but could not reload order"}};
        }
        return updated;
    }
}
