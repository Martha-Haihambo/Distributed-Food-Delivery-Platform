import ballerina/http;
import ballerina/log;

listener http:Listener notificationListener = new (8086);

service /notifications on notificationListener {

    // GET /notifications?orderId= — audit/history endpoint (a real system would key
    // this by userId across channels; keyed by orderId here since that's the
    // correlation id every event carries).
    resource function get .(string orderId) returns NotificationRecord[]|http:InternalServerError {
        NotificationRecord[]|error result = listNotificationsForOrder(orderId);
        if result is error {
            log:printError("failed to list notifications", result);
            return <http:InternalServerError>{body: {message: "could not list notifications"}};
        }
        return result;
    }
}
