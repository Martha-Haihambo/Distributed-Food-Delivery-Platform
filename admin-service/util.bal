import ballerina/time;

function currentIso() returns string {
    return time:utcToString(time:utcNow());
}
