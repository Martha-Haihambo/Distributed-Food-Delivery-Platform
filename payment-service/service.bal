import ballerina/http;

listener http:Listener paymentListener = new (8084);

service /payments on paymentListener {

    // GET /payments/{orderId} — transaction lookup for support/audit purposes.
    resource function get [string orderId]() returns Transaction|http:NotFound {
        Transaction|error t = getTransaction(orderId);
        if t is error {
            return <http:NotFound>{body: {message: string `no transaction found for order ${orderId}`}};
        }
        return t;
    }
}
