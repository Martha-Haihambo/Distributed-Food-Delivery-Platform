import ballerina/http;
import ballerina/log;

listener http:Listener customerListener = new (8081);

service /customers on customerListener {

    // POST /customers
    resource function post .(NewCustomer nc) returns Customer|http:InternalServerError {
        Customer|error c = createCustomer(nc);
        if c is error {
            log:printError("failed to create customer", c);
            return <http:InternalServerError>{body: {message: "could not create customer"}};
        }
        return c;
    }

    // GET /customers/{id}
    resource function get [int id]() returns Customer|http:NotFound {
        Customer|error c = getCustomer(id);
        if c is error {
            return <http:NotFound>{body: {message: string `customer ${id} not found`}};
        }
        return c;
    }

    // POST /customers/{id}/addresses
    resource function post [int id]/addresses(NewAddress addr) returns Address|http:InternalServerError {
        Address|error a = addAddress(id, addr);
        if a is error {
            log:printError("failed to add address", a);
            return <http:InternalServerError>{body: {message: "could not add address"}};
        }
        return a;
    }

    // GET /customers/{id}/orders — served from this service's own read-model, kept
    // current by consuming order-lifecycle events (see kafka.bal), not by calling
    // Order Service synchronously.
    resource function get [int id]/orders() returns OrderHistoryEntry[]|http:InternalServerError {
        OrderHistoryEntry[]|error history = getOrderHistory(id);
        if history is error {
            log:printError("failed to load order history", history);
            return <http:InternalServerError>{body: {message: "could not load order history"}};
        }
        return history;
    }
}
