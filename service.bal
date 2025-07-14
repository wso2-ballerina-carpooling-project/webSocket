import webSocketService.common;
import lakpahana/firebase_realtime_database;
import ballerina/io;
import ballerina/log;
import ballerina/time;
import ballerina/websocket;


configurable int wsport = ?;
configurable string host = ?;

map<websocket:Caller> connectedDrivers = {};
map<common:DriverInfo> driverInfoMap = {};

// Firebase client - initialized at module level
configurable string firebaseApiKey = ?;
configurable string firebaseAuthDomain = ?;
configurable string firebaseDatabaseURL = ?;
configurable string firebaseProjectId = ?;
configurable string firebaseStorageBucket = ?;
configurable string firebaseMessagingSenderId = ?;
configurable string firebaseAppId = ?;
configurable string firebaseMeasurementId = ?;

json firebaseConfigJson = {
    apiKey: firebaseApiKey,
    authDomain: firebaseAuthDomain,
    databaseURL: firebaseDatabaseURL,
    projectId: firebaseProjectId,
    storageBucket: firebaseStorageBucket,
    messagingSenderId: firebaseMessagingSenderId,
    appId: firebaseAppId,
    measurementId: firebaseMeasurementId
};

// Initialize Firebase on module load
public function main() returns error? {
    io:println("=== LOCATION TRACKING WEBSOCKET SERVICE ===");
    io:println("Starting WebSocket service on port: " + wsport.toString());
    io:println("WebSocket endpoint: ws://" + host + ":" + wsport.toString() + "/ws");
    io:println("==========================================");

    getCurrentDriverStatus();
}

service /ws on new websocket:Listener(wsport) {
    resource function get .() returns websocket:Service|websocket:UpgradeError {
        log:printInfo("New WebSocket connection request received");
        return new LocationWebSocketService();
    }
}

public isolated service class LocationWebSocketService {
    *websocket:Service;

    remote function onOpen(websocket:Caller caller) returns websocket:Error? {
        log:printInfo("WebSocket connection opened");

        json welcomeMessage = {
            "type": "connection_established",
            "message": "Welcome to the location tracking service",
            "timestamp": time:utcNow()
        };

        check caller->writeMessage(welcomeMessage);
    }

    remote function onMessage(websocket:Caller caller, string|json|byte[]|xml message) returns websocket:Error? {
        if message is string {
            log:printInfo("Received text message from client");
            json|error jsonMessage = message.fromJsonString();

            if jsonMessage is json {
                check handleJsonMessage(caller, jsonMessage);
            } else {
                log:printError("Failed to parse JSON message", jsonMessage);
                check sendErrorResponse(caller, "Invalid JSON format");
            }
        } else if message is json {
            log:printInfo("Received JSON message from client");
            check handleJsonMessage(caller, message);
        } else {
            log:printWarn("Received unsupported message type");
            check sendErrorResponse(caller, "Unsupported message type");
        }
    }

    remote function onClose(websocket:Caller caller, int statusCode, string reason) {
        log:printInfo(string `WebSocket connection closed. Status: ${statusCode}, Reason: ${reason}`);

        lock {
            string? driverIdToRemove = ();
            foreach var [key, value] in connectedDrivers.entries() {
                if value === caller {
                    driverIdToRemove = key;
                    break;
                }
            }

            if driverIdToRemove is string {
                _ = connectedDrivers.remove(driverIdToRemove);
                _ = driverInfoMap.remove(driverIdToRemove);
                
                // Update Firebase - mark driver as disconnected
                error? disconnectResult = updateDriverConnectionStatus(driverIdToRemove, false);
                if (disconnectResult is error) {
                    log:printError("Failed to update driver disconnect status in Firebase", disconnectResult);
                }
                
                log:printInfo("Driver removed from connected drivers: " + driverIdToRemove);
            }
        }
    }

    remote function onError(websocket:Caller caller, websocket:Error err) {
        log:printError("WebSocket error occurred", err);
    }
}

function handleJsonMessage(websocket:Caller caller, json message) returns websocket:Error? {
    var messageType = message.'type;

    if messageType is string {
        match messageType {
            "driver_connected" => {
                check handleDriverConnected(caller, message);
            }
            "location_update" => {
                check handleLocationUpdate(caller, message);
            }
            "heartbeat" => {
                check handleHeartbeat(caller, message);
            }
            "waypoint_approaching" => {
                check handleWaypointApproaching(caller, message);
            }
            "pickup_arrival" => {
                check handlePickupArrival(caller, message);
            }
            "passenger_picked_up" => {
                check handlePassengerPickedUp(caller, message);
            }
            "driver_disconnected" => {
                check handleDriverDisconnected(caller, message);
            }
            _ => {
                log:printWarn("Unknown message type received: " + messageType);
                check sendErrorResponse(caller, "Unknown message type");
            }
        }
    } else {
        log:printError("Invalid message format - missing type field");
        check sendErrorResponse(caller, "Invalid message format");
    }
}

function handleDriverConnected(websocket:Caller caller, json message) returns websocket:Error? {
    common:DriverConnectedMessage|error parseResult = message.cloneWithType();

    if parseResult is common:DriverConnectedMessage {
        string driverId = parseResult.driver_id;
        string rideId = parseResult.ride_id;

        lock {
            connectedDrivers[driverId] = caller;
            driverInfoMap[driverId] = {
                driverId: driverId,
                rideId: rideId,
                connectionTime: parseResult.timestamp,
                lastLatitude: 0,
                lastLocationUpdate: "",
                lastLongitude: 0
            };
        }

        io:println("=== DRIVER CONNECTED ===");
        io:println("Driver ID: " + driverId);
        io:println("Ride ID: " + rideId);
        io:println("Connection Time: " + parseResult.timestamp);
        io:println("Total Connected Drivers: " + connectedDrivers.length().toString());
        io:println("========================");
        
        // Store driver connection in Firebase
        json driverConnectionData = {
            "driver_id": driverId,
            "ride_id": rideId,
            "connection_time": parseResult.timestamp,
            "status": "connected",
            "last_updated": time:utcNow()
        };
        
        error? putResult = storeDriverConnection(driverId, driverConnectionData);
        if (putResult is error) {
            log:printError("Failed to store driver connection in Firebase", putResult);
        } else {
            log:printInfo("Driver connection stored in Firebase successfully");
        }
        
        json response = {
            "type": "driver_connected_ack",
            "driver_id": driverId,
            "status": "success",
            "timestamp": time:utcNow()
        };

        check caller->writeMessage(response);
    } else {
        log:printError("Invalid driver connected message format", parseResult);
        check sendErrorResponse(caller, "Invalid driver connected message format");
    }
}

function handleLocationUpdate(websocket:Caller caller, json message) returns websocket:Error? {
    common:LocationUpdateMessage|error parseResult = message.cloneWithType();

    if parseResult is common:LocationUpdateMessage {
        string driverId = parseResult.driver_id;
        string rideId = parseResult.ride_id;
        decimal latitude = parseResult.latitude;
        decimal longitude = parseResult.longitude;

        lock {
            if driverInfoMap.hasKey(driverId) {
                common:DriverInfo driverInfo = driverInfoMap.get(driverId);
                driverInfo.lastLatitude = latitude;
                driverInfo.lastLongitude = longitude;
                driverInfo.lastLocationUpdate = parseResult.timestamp;
                driverInfoMap[driverId] = driverInfo;
            }
        }

        io:println("=== LOCATION UPDATE ===");
        io:println("Driver ID: " + driverId);
        io:println("Ride ID: " + rideId);
        io:println("Latitude: " + latitude.toString());
        io:println("Longitude: " + longitude.toString());

        // Store location update in Firebase
        json locationData = {
            "driver_id": driverId,
            "ride_id": rideId,
            "latitude": latitude,
            "longitude": longitude,
            "timestamp": parseResult.timestamp,
            "speed": parseResult.speed is decimal ? parseResult.speed : null,
            "heading": parseResult.heading is decimal ? parseResult.heading : null,
            "accuracy": parseResult.accuracy is decimal ? parseResult.accuracy : null
        };

        error? locationResult = storeLocationUpdate(driverId, locationData);
        if (locationResult is error) {
            log:printError("Failed to store location update in Firebase", locationResult);
        } else {
            log:printInfo("Location update stored in Firebase successfully");
        }

        if parseResult.speed is decimal {
            io:println("Speed: " + parseResult.speed.toString() + " m/s");
        }
        if parseResult.heading is decimal {
            io:println("Heading: " + parseResult.heading.toString() + "°");
        }
        if parseResult.accuracy is decimal {
            io:println("Accuracy: " + parseResult.accuracy.toString() + " meters");
        }

        io:println("Timestamp: " + parseResult.timestamp);
        io:println("======================");

        json response = {
            "type": "location_received",
            "driver_id": driverId,
            "status": "success",
            "timestamp": time:utcNow()
        };

        check caller->writeMessage(response);
    } else {
        log:printError("Invalid location update message format", parseResult);
        check sendErrorResponse(caller, "Invalid location update message format");
    }
}

function handleHeartbeat(websocket:Caller caller, json message) returns websocket:Error? {
    common:HeartbeatMessage|error parseResult = message.cloneWithType();

    if parseResult is common:HeartbeatMessage {
        string driverId = parseResult.driver_id;

        io:println("=== HEARTBEAT ===");
        io:println("Driver ID: " + driverId);
        io:println("Timestamp: " + parseResult.timestamp);
        io:println("=================");

        // Update heartbeat in Firebase
        json heartbeatData = {
            "driver_id": driverId,
            "last_heartbeat": parseResult.timestamp,
            "status": "active"
        };

        error? heartbeatResult = updateDriverHeartbeat(driverId, heartbeatData);
        if (heartbeatResult is error) {
            log:printError("Failed to update heartbeat in Firebase", heartbeatResult);
        }

        json response = {
            "type": "heartbeat_ack",
            "driver_id": driverId,
            "server_timestamp": time:utcNow()
        };

        check caller->writeMessage(response);
    } else {
        log:printError("Invalid heartbeat message format", parseResult);
        check sendErrorResponse(caller, "Invalid heartbeat message format");
    }
}

function handleWaypointApproaching(websocket:Caller caller, json message) returns websocket:Error? {
    common:WaypointApproachingMessage|error parseResult = message.cloneWithType();

    if parseResult is common:WaypointApproachingMessage {
        string driverId = parseResult.driver_id;
        string rideId = parseResult.ride_id;

        io:println("=== WAYPOINT APPROACHING ===");
        io:println("Driver ID: " + driverId);
        io:println("Ride ID: " + rideId);
        io:println("Waypoint Latitude: " + parseResult.waypoint_latitude.toString());
        io:println("Waypoint Longitude: " + parseResult.waypoint_longitude.toString());
        io:println("Distance to Waypoint: " + parseResult.distance_to_waypoint.toString() + " meters");
        io:println("Timestamp: " + parseResult.timestamp);
        io:println("============================");

        // Store waypoint event in Firebase
        json waypointData = {
            "driver_id": driverId,
            "ride_id": rideId,
            "event_type": "waypoint_approaching",
            "waypoint_latitude": parseResult.waypoint_latitude,
            "waypoint_longitude": parseResult.waypoint_longitude,
            "distance_to_waypoint": parseResult.distance_to_waypoint,
            "timestamp": parseResult.timestamp
        };

        error? waypointResult = storeWaypointEvent(driverId, waypointData);
        if (waypointResult is error) {
            log:printError("Failed to store waypoint event in Firebase", waypointResult);
        }

        json response = {
            "type": "waypoint_approaching_ack",
            "driver_id": driverId,
            "status": "received",
            "timestamp": time:utcNow()
        };

        check caller->writeMessage(response);
    } else {
        log:printError("Invalid waypoint approaching message format", parseResult);
        check sendErrorResponse(caller, "Invalid waypoint approaching message format");
    }
}

function handlePickupArrival(websocket:Caller caller, json message) returns websocket:Error? {
    common:PickupArrivalMessage|error parseResult = message.cloneWithType();

    if parseResult is common:PickupArrivalMessage {
        string driverId = parseResult.driver_id;
        string rideId = parseResult.ride_id;
        string passengerName = parseResult.passenger_name;

        io:println("=== PICKUP ARRIVAL ===");
        io:println("Driver ID: " + driverId);
        io:println("Ride ID: " + rideId);
        io:println("Passenger Name: " + passengerName);
        io:println("Timestamp: " + parseResult.timestamp);
        io:println("======================");

        // Store pickup arrival event in Firebase
        json pickupData = {
            "driver_id": driverId,
            "ride_id": rideId,
            "event_type": "pickup_arrival",
            "passenger_name": passengerName,
            "timestamp": parseResult.timestamp
        };

        error? pickupResult = storeRideEvent(driverId, rideId, pickupData);
        if (pickupResult is error) {
            log:printError("Failed to store pickup arrival in Firebase", pickupResult);
        }

        json response = {
            "type": "pickup_arrival_ack",
            "driver_id": driverId,
            "status": "received",
            "timestamp": time:utcNow()
        };

        check caller->writeMessage(response);
    } else {
        log:printError("Invalid pickup arrival message format", parseResult);
        check sendErrorResponse(caller, "Invalid pickup arrival message format");
    }
}

function handlePassengerPickedUp(websocket:Caller caller, json message) returns websocket:Error? {
    common:PassengerPickedUpMessage|error parseResult = message.cloneWithType();

    if parseResult is common:PassengerPickedUpMessage {
        string driverId = parseResult.driver_id;
        string rideId = parseResult.ride_id;
        string passengerName = parseResult.passenger_name;

        io:println("=== PASSENGER PICKED UP ===");
        io:println("Driver ID: " + driverId);
        io:println("Ride ID: " + rideId);
        io:println("Passenger Name: " + passengerName);
        io:println("Timestamp: " + parseResult.timestamp);
        io:println("===========================");

        // Store passenger pickup event in Firebase
        json pickupData = {
            "driver_id": driverId,
            "ride_id": rideId,
            "event_type": "passenger_picked_up",
            "passenger_name": passengerName,
            "timestamp": parseResult.timestamp
        };

        error? pickupResult = storeRideEvent(driverId, rideId, pickupData);
        if (pickupResult is error) {
            log:printError("Failed to store passenger pickup in Firebase", pickupResult);
        }

        json response = {
            "type": "passenger_picked_up_ack",
            "driver_id": driverId,
            "status": "received",
            "timestamp": time:utcNow()
        };

        check caller->writeMessage(response);
    } else {
        log:printError("Invalid passenger picked up message format", parseResult);
        check sendErrorResponse(caller, "Invalid passenger picked up message format");
    }
}

function handleDriverDisconnected(websocket:Caller caller, json message) returns websocket:Error? {
    common:DriverDisconnectedMessage|error parseResult = message.cloneWithType();

    if parseResult is common:DriverDisconnectedMessage {
        string driverId = parseResult.driver_id;
        string rideId = parseResult.ride_id;

        io:println("=== DRIVER DISCONNECTED ===");
        io:println("Driver ID: " + driverId);
        io:println("Ride ID: " + rideId);
        io:println("Timestamp: " + parseResult.timestamp);
        io:println("===========================");

        lock {
            _ = connectedDrivers.remove(driverId);
            _ = driverInfoMap.remove(driverId);
        }

        // Update Firebase - mark driver as disconnected
        error? disconnectResult = updateDriverConnectionStatus(driverId, false);
        if (disconnectResult is error) {
            log:printError("Failed to update driver disconnect status in Firebase", disconnectResult);
        }

        io:println("Remaining Connected Drivers: " + connectedDrivers.length().toString());
    } else {
        log:printError("Invalid driver disconnected message format", parseResult);
    }
}

function sendErrorResponse(websocket:Caller caller, string errorMessage) returns websocket:Error? {
    json errorResponse = {
        "type": "error",
        "message": errorMessage,
        "timestamp": time:utcNow()
    };

    check caller->writeMessage(errorResponse);
}

public function getCurrentDriverStatus() {
    io:println("=== CURRENT DRIVER STATUS ===");
    lock {
        if connectedDrivers.length() == 0 {
            io:println("No drivers currently connected");
        } else {
            io:println("Connected Drivers: " + connectedDrivers.length().toString());

            foreach var [driverId, driverInfo] in driverInfoMap.entries() {
                io:println("Driver ID: " + driverId);
                io:println("Ride ID: " + driverInfo.rideId);
                io:println("Connection Time: " + driverInfo.connectionTime);

                if driverInfo.lastLatitude is decimal && driverInfo.lastLongitude is decimal {
                    io:println("Last Location: " + driverInfo.lastLatitude.toString() + ", " + driverInfo.lastLongitude.toString());
                }

                if driverInfo.lastLocationUpdate is string {
                    io:println(driverInfo.lastLocationUpdate);
                }

                io:println("---");
            }
        }
    }
    io:println("=============================");
}

function broadcastToAllDrivers(json message) returns websocket:Error? {
    lock {
        foreach var [driverId, caller] in connectedDrivers.entries() {
            websocket:Error? result = caller->writeMessage(message);
            if result is websocket:Error {
                log:printError("Error broadcasting message to driver: " + driverId, result);
            }
        }
    }
}

// Firebase helper functions
function storeDriverConnection(string driverId, json data) returns error? {


}

function storeLocationUpdate(string driverId, json data) returns error? {

    
    // Store in both current location and location history
    string currentPath = "driver_locations/" + driverId + "/current";
    firebase_realtime_database:FirebaseDatabaseClient firebaseClient = check new (firebaseConfigJson, "service-account.json");
    error? putError = firebaseClient.putData(currentPath, data);
    if (putError != null) {
        log:printError("Failed to put data:", putError);
    }
}

function updateDriverHeartbeat(string driverId, json data) returns error? {
    string path = "driver_heartbeats/" + driverId;
    firebase_realtime_database:FirebaseDatabaseClient firebaseClient = check new (firebaseConfigJson, "service-account.json");
    error? putError = firebaseClient.putData(path, data);
    if (putError != null) {
        log:printError("Failed to put data:", putError);
    }
}

function updateDriverConnectionStatus(string driverId, boolean isConnected) returns error? {

    json statusData = {
        "driver_id": driverId,
        "is_connected": isConnected,
        "last_updated": time:utcNow(),
        "status": isConnected ? "connected" : "disconnected"
    };
    
    string path = "driver_connections/" + driverId + "/status";
    firebase_realtime_database:FirebaseDatabaseClient firebaseClient = check new (firebaseConfigJson, "service-account.json");
    error? putError = firebaseClient.putData(path, statusData);
    if (putError != null) {
        log:printError("Failed to put data:", putError);
    }

}

function storeWaypointEvent(string driverId, json data) returns error? {

    string path = "ride_events/" + driverId + "/waypoints/" + time:utcNow().toString();
    firebase_realtime_database:FirebaseDatabaseClient firebaseClient = check new (firebaseConfigJson, "service-account.json");
    error? putError = firebaseClient.putData(path, data);
    if (putError != null) {
        log:printError("Failed to put data:", putError);
    }

}

function storeRideEvent(string driverId, string rideId, json data)  {


}
