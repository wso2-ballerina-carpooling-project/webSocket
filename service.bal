import webSocketService.common;

import ballerina/http;
import ballerina/io;
import ballerina/jwt;
import ballerina/log;
import ballerina/oauth2;
import ballerina/time;
import ballerina/websocket;

configurable int wsport = ?;
configurable string host = ?;

// Thread-safe maps for driver management
map<websocket:Caller> connectedDrivers = {};
map<common:DriverInfo> driverInfoMap = {};
map<websocket:Caller> connectedPassengers = {};
map<common:PassengerInfo> passengerInfoMap = {};

// Firebase configuration
configurable string firebaseProjectId = ?;
configurable string firebaseDatabaseURL = ?;

// Service account configuration for authentication
configurable string serviceAccountPath = ?;
configurable string privateKeyPath = ?;
configurable string jwtScope = "https://www.googleapis.com/auth/firebase.database https://www.googleapis.com/auth/userinfo.email";

// HTTP client for Firebase REST API with timeout configuration
http:Client firebaseClient = check new (firebaseDatabaseURL, {
    timeout: 30,
    retryConfig: {
        count: 3,
        interval: 2
    }
});

// Firebase authentication client
Client authClient = check new ({
    serviceAccount: check getServiceAccount(serviceAccountPath),
    jwtConfig: {
        scope: jwtScope,
        expTime: 3600
    },
    privateKeyPath: privateKeyPath
});

// Service account type
type ServiceAccount record {|
    string 'type;
    string project_id;
    string private_key_id;
    string private_key;
    string client_email;
    string client_id;
    string auth_uri;
    string token_uri;
    string auth_provider_x509_cert_url;
    string client_x509_cert_url;
    string universe_domain;
|};

// JWT configuration type
type JWTConfig record {|
    string scope;
    decimal expTime;
|};

// Enhanced Auth client with better error handling
public client class Client {
    private ServiceAccount? serviceAccount;
    private string? jwt = ();
    private JWTConfig? jwtConfig = ();
    private final string PRIVATE_KEY_PATH;

    public isolated function init(record {ServiceAccount serviceAccount; JWTConfig jwtConfig; string privateKeyPath;} authConfig) returns error? {
        self.serviceAccount = authConfig.serviceAccount;
        self.jwtConfig = authConfig.jwtConfig;
        self.PRIVATE_KEY_PATH = authConfig.privateKeyPath;
        return;
    }

    isolated function generateJWT(ServiceAccount serviceAccount) returns string|error {
        lock {
            JWTConfig? jwtConfig = self.jwtConfig;
            if jwtConfig is () {
                return error("JWT Config is not provided");
            }

            int timeNow = time:utcNow()[0];
            int expTime = timeNow + <int>jwtConfig.expTime;

            jwt:IssuerConfig issuerConfig = {
                issuer: serviceAccount.client_email,
                audience: serviceAccount.token_uri,
                expTime: jwtConfig.expTime,
                signatureConfig: {
                    algorithm: jwt:RS256,
                    config: {
                        keyFile: self.PRIVATE_KEY_PATH
                    }
                },
                customClaims: {
                    iss: serviceAccount.client_email,
                    scope: jwtConfig.scope,
                    aud: serviceAccount.token_uri,
                    iat: timeNow,
                    exp: expTime
                }
            };

            string jwtToken = check jwt:issue(issuerConfig);
            self.jwt = jwtToken;
            return jwtToken;
        }
    }

    isolated function isJWTExpired(string jwtToken) returns boolean|error {
        [jwt:Header, jwt:Payload] [_, payload] = check jwt:decode(jwtToken);
        int? exp = payload.exp;
        if exp is int {
            int timeNow = time:utcNow()[0];
            return exp < timeNow;
        }
        return error("Error in decoding JWT - missing exp claim");
    }

    public isolated function generateToken() returns string|error {
        lock {
            ServiceAccount? serviceAccount = self.serviceAccount.cloneReadOnly();
            if serviceAccount is () {
                return error("Service Account is not provided");
            }

            string currentJWT = self.jwt ?: "";

            // Generate new JWT if none exists or if current one is expired
            if currentJWT == "" || check self.isJWTExpired(currentJWT) {
                currentJWT = check self.generateJWT(serviceAccount);
            }

            oauth2:JwtBearerGrantConfig jwtBearerGrantConfig = {
                tokenUrl: serviceAccount.token_uri,
                assertion: currentJWT
            };

            oauth2:ClientOAuth2Provider oauth2Provider = new (jwtBearerGrantConfig);
            return oauth2Provider.generateToken();
        }
    }
}

function getServiceAccount(string path) returns ServiceAccount|error {
    json serviceAccountFileInput = check io:fileReadJson(path);
    return check serviceAccountFileInput.cloneWithType(ServiceAccount);
}

public function main() returns error? {
    io:println("=== LOCATION TRACKING WEBSOCKET SERVICE ===");
    io:println("Starting WebSocket service on port: " + wsport.toString());
    io:println("WebSocket endpoint: ws://" + host + ":" + wsport.toString() + "/ws");
    io:println("Firebase Project ID: " + firebaseProjectId);
    io:println("Firebase Database URL: " + firebaseDatabaseURL);
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
            "timestamp": time:utcNow(),
            "server_version": "1.0.0"
        };

        check caller->writeMessage(welcomeMessage);
    }

    remote function onMessage(websocket:Caller caller, string|json|byte[]|xml message) returns websocket:Error? {
        if message is string {
            log:printInfo("Received text message from client");
            json|error jsonMessage = message.fromJsonString();
            io:print(jsonMessage);
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
        cleanupDriverConnection(caller);
    }

    remote function onError(websocket:Caller caller, websocket:Error err) {
        log:printError("WebSocket error occurred", err);
        cleanupDriverConnection(caller);
    }
}

function cleanupDriverConnection(websocket:Caller caller) {
    lock {
        string? driverIdToRemove = ();

        // Find driver ID by caller reference
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
            var disconnectResult = updateDriverConnectionStatus(driverIdToRemove, false);
            if disconnectResult is error {
                log:printError("Failed to update driver disconnect status in Firebase", disconnectResult);
            }

            log:printInfo("Driver removed from connected drivers: " + driverIdToRemove);
        }
    }
}

function handleJsonMessage(websocket:Caller caller, json message) returns websocket:Error? {
    var messageType = message.'type;
    io:print(messageType);
    if messageType is string {
        match messageType {
            "driver_connected" => {
                return handleDriverConnected(caller, message);
            }
            "location_update" => {
                return handleLocationUpdateWithPassengerNotification(caller, message);
            }
            "heartbeat" => {
                return handleHeartbeat(caller, message);
            }
            "waypoint_approaching" => {
                return handleWaypointApproaching(caller, message);
            }
            "pickup_arrival" => {
                return handlePickupArrival(caller, message);
            }
            "passenger_picked_up" => {
                return handlePassengerPickedUp(caller, message);
            }
            "passenger_connected" => {
                return handlePassengerConnected(caller,message);
            }
            "driver_disconnected" => {
                return handleDriverDisconnected(caller, message);
            }
            _ => {
                log:printWarn("Unknown message type received: " + messageType);
                return sendErrorResponse(caller, "Unknown message type: " + messageType);
            }
        }
    } else {
        log:printError("Invalid message format - missing type field");
        return sendErrorResponse(caller, "Invalid message format - missing type field");
    }
}

function handleDriverConnected(websocket:Caller caller, json message) returns websocket:Error? {
    common:DriverConnectedMessage|error parseResult = message.cloneWithType();

    if parseResult is common:DriverConnectedMessage {
        string driverId = parseResult.driver_id;
        string rideId = parseResult.ride_id;

        // Check if driver is already connected
        lock {
            if connectedDrivers.hasKey(driverId) {
                log:printWarn("Driver already connected: " + driverId);
                return sendErrorResponse(caller, "Driver already connected");
            }

            connectedDrivers[driverId] = caller;
            driverInfoMap[driverId] = {
                driverId: driverId,
                rideId: rideId,
                connectionTime: parseResult.timestamp,
                lastLatitude: 0,
                lastLocationUpdate: "",
                lastLongitude: 0,
                lastSeen: 0
            };
        }

        io:println("=== DRIVER CONNECTED ===");
        io:println("Driver ID: " + driverId);
        io:println("Ride ID: " + rideId);
        io:println("Connection Time: " + parseResult.timestamp);
        io:println("Total Connected Drivers: " + connectedDrivers.length().toString());
        io:println("========================");

        // Store driver connection in Firebase asynchronously
        var _ = start storeDriverConnectionAsync(driverId, {
            "driver_id": driverId,
            "ride_id": rideId,
            "connection_time": parseResult.timestamp,
            "status": "connected",
            "last_updated": time:utcNow()
        });

        json response = {
            "type": "driver_connected_ack",
            "driver_id": driverId,
            "status": "success",
            "timestamp": time:utcNow()
        };

        return caller->writeMessage(response);
    } else {
        log:printError("Invalid driver connected message format", parseResult);
        return sendErrorResponse(caller, "Invalid driver connected message format");
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
            } else {
                log:printWarn("Location update for unknown driver: " + driverId);
                return sendErrorResponse(caller, "Driver not registered");
            }
        }

        io:println("=== LOCATION UPDATE ===");
        io:println("Driver ID: " + driverId);
        io:println("Ride ID: " + rideId);
        io:println("Latitude: " + latitude.toString());
        io:println("Longitude: " + longitude.toString());

        // Store location update in Firebase asynchronously
        json locationData = {
            "driver_id": driverId,
            "ride_id": rideId,
            "latitude": latitude,
            "longitude": longitude,
            "timestamp": parseResult.timestamp,
            "speed": parseResult.speed is decimal ? parseResult.speed : (),
            "heading": parseResult.heading is decimal ? parseResult.heading : (),
            "accuracy": parseResult.accuracy is decimal ? parseResult.accuracy : ()
        };

        var _ = start storeLocationUpdateAsync(driverId, locationData);

        // Log optional fields
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

        return caller->writeMessage(response);
    } else {
        log:printError("Invalid location update message format", parseResult);
        return sendErrorResponse(caller, "Invalid location update message format");
    }
}

function handleHeartbeat(websocket:Caller caller, json message) returns websocket:Error? {
    common:HeartbeatMessage|error parseResult = message.cloneWithType();

    if parseResult is common:HeartbeatMessage {
        string driverId = parseResult.driver_id;

        // Verify driver is connected
        lock {
            if !connectedDrivers.hasKey(driverId) {
                return sendErrorResponse(caller, "Driver not registered");
            }
        }

        io:println("=== HEARTBEAT ===");
        io:println("Driver ID: " + driverId);
        io:println("Timestamp: " + parseResult.timestamp);
        io:println("=================");

        // Update heartbeat in Firebase asynchronously
        var _ = start updateDriverHeartbeatAsync(driverId, {
            "driver_id": driverId,
            "last_heartbeat": parseResult.timestamp,
            "status": "active"
        });

        json response = {
            "type": "heartbeat_ack",
            "driver_id": driverId,
            "server_timestamp": time:utcNow()
        };

        return caller->writeMessage(response);
    } else {
        log:printError("Invalid heartbeat message format", parseResult);
        return sendErrorResponse(caller, "Invalid heartbeat message format");
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

        // Store waypoint event in Firebase asynchronously
        var _ = start storeWaypointEventAsync(driverId, {
                                                            "driver_id": driverId,
                                                            "ride_id": rideId,
                                                            "event_type": "waypoint_approaching",
                                                            "waypoint_latitude": parseResult.waypoint_latitude,
                                                            "waypoint_longitude": parseResult.waypoint_longitude,
                                                            "distance_to_waypoint": parseResult.distance_to_waypoint,
                                                            "timestamp": parseResult.timestamp
                                                        });

        json response = {
            "type": "waypoint_approaching_ack",
            "driver_id": driverId,
            "status": "received",
            "timestamp": time:utcNow()
        };

        return caller->writeMessage(response);
    } else {
        log:printError("Invalid waypoint approaching message format", parseResult);
        return sendErrorResponse(caller, "Invalid waypoint approaching message format");
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

        // Store pickup arrival event in Firebase asynchronously
        var _ = start storeRideEventAsync(driverId, rideId, {
                                                                "driver_id": driverId,
                                                                "ride_id": rideId,
                                                                "event_type": "pickup_arrival",
                                                                "passenger_name": passengerName,
                                                                "timestamp": parseResult.timestamp
                                                            });

        json response = {
            "type": "pickup_arrival_ack",
            "driver_id": driverId,
            "status": "received",
            "timestamp": time:utcNow()
        };

        return caller->writeMessage(response);
    } else {
        log:printError("Invalid pickup arrival message format", parseResult);
        return sendErrorResponse(caller, "Invalid pickup arrival message format");
    }
}

function handlePassengerPickedUp(websocket:Caller caller, json message) returns websocket:Error? {
    common:PassengerPickedUpMessage|error parseResult = message.cloneWithType();

    if parseResult is common:PassengerPickedUpMessage {
        string driverId = parseResult.driver_id;
        string rideId = parseResult.ride_id;
        string passengerId = parseResult.passenger_id;

        io:println("=== PASSENGER PICKED UP ===");
        io:println("Driver ID: " + driverId);
        io:println("Ride ID: " + rideId);
        io:println("Passenger ID: " + passengerId);
        io:println("Timestamp: " + parseResult.timestamp);
        io:println("===========================");


        notifyPassengerPickedUp(passengerId, driverId, rideId, parseResult.timestamp);
        // Store passenger pickup event in Firebase asynchronously
        var _ = start storeRideEventAsync(driverId, rideId, {
                                                                "driver_id": driverId,
                                                                "ride_id": rideId,
                                                                "event_type": "passenger_picked_up",
                                                                "passenger_name": passengerId,
                                                                "timestamp": parseResult.timestamp
                                                            });

        json response = {
            "type": "passenger_picked_up_ack",
            "driver_id": driverId,
            "status": "received",
            "timestamp": time:utcNow()
        };

        return caller->writeMessage(response);
    } else {
        log:printError("Invalid passenger picked up message format", parseResult);
        return sendErrorResponse(caller, "Invalid passenger picked up message format");
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

        // Update Firebase - mark driver as disconnected asynchronously
        var _ = start updateDriverConnectionStatusAsync(driverId, false);

        io:println("Remaining Connected Drivers: " + connectedDrivers.length().toString());
    } else {
        log:printError("Invalid driver disconnected message format", parseResult);
        return sendErrorResponse(caller, "Invalid driver disconnected message format");
    }
}

function sendErrorResponse(websocket:Caller caller, string errorMessage) returns websocket:Error? {
    json errorResponse = {
        "type": "error",
        "message": errorMessage,
        "timestamp": time:utcNow()
    };

    return caller->writeMessage(errorResponse);
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

                io:println("---");
            }
        }
    }
    io:println("=============================");
}

function broadcastToAllDrivers(json message) {
    lock {
        foreach var [driverId, caller] in connectedDrivers.entries() {
            var result = caller->writeMessage(message);
            if result is websocket:Error {
                log:printError("Error broadcasting message to driver: " + driverId, result);
            }
        }
    }
}

// Async Firebase operations
function storeDriverConnectionAsync(string driverId, json data) {
    var result = storeDriverConnection(driverId, data);
    if result is error {
        log:printError("Failed to store driver connection in Firebase", result);
    } else {
        log:printInfo("Driver connection stored in Firebase successfully");
    }
}

function storeLocationUpdateAsync(string driverId, json data) {
    var result = storeLocationUpdate(driverId, data);
    if result is error {
        log:printError("Failed to store location update in Firebase", result);
    } else {
        log:printInfo("Location update stored in Firebase successfully");
    }
}

function updateDriverHeartbeatAsync(string driverId, json data) {
    var result = updateDriverHeartbeat(driverId, data);
    if result is error {
        log:printError("Failed to update heartbeat in Firebase", result);
    }
}

function storeWaypointEventAsync(string driverId, json data) {
    var result = storeWaypointEvent(driverId, data);
    if result is error {
        log:printError("Failed to store waypoint event in Firebase", result);
    }
}

function storeRideEventAsync(string driverId, string rideId, json data) {
    var result = storeRideEvent(driverId, rideId, data);
    if result is error {
        log:printError("Failed to store ride event in Firebase", result);
    }
}

function updateDriverConnectionStatusAsync(string driverId, boolean isConnected) {
    var result = updateDriverConnectionStatus(driverId, isConnected);
    if result is error {
        log:printError("Failed to update driver connection status in Firebase", result);
    }
}

// Firebase REST API helper functions
function getAccessToken() returns string|error {
    return authClient.generateToken();
}

function storeDriverConnection(string driverId, json data) returns error? {
    string accessToken = check getAccessToken();
    string path = "/driver_connections/" + driverId + ".json";

    http:Response response = check firebaseClient->put(path, data, {
        "Authorization": "Bearer " + accessToken
    });

    if response.statusCode != 200 {
        return error("Failed to store driver connection: " + response.statusCode.toString());
    }
}

function storeLocationUpdate(string driverId, json data) returns error? {
    string accessToken = check getAccessToken();

    // Store in current location
    string currentPath = "/driver_locations/" + driverId + "/current.json";
    http:Response currentResponse = check firebaseClient->put(currentPath, data, {
        "Authorization": "Bearer " + accessToken
    });

    if currentResponse.statusCode != 200 {
        return error("Failed to store current location: " + currentResponse.statusCode.toString());
    }

    // Store in location history with proper timestamp
    string timestamp = time:utcNow().toString();
    string historyPath = "/driver_locations/" + driverId + "/history/" + timestamp + ".json";
    http:Response historyResponse = check firebaseClient->put(historyPath, data, {
        "Authorization": "Bearer " + accessToken
    });

    if historyResponse.statusCode != 200 {
        return error("Failed to store location history: " + historyResponse.statusCode.toString());
    }
}

function updateDriverHeartbeat(string driverId, json data) returns error? {
    string accessToken = check getAccessToken();
    string path = "/driver_heartbeats/" + driverId + ".json";

    http:Response response = check firebaseClient->put(path, data, {
        "Authorization": "Bearer " + accessToken
    });

    if response.statusCode != 200 {
        return error("Failed to update heartbeat: " + response.statusCode.toString());
    }
}

function updateDriverConnectionStatus(string driverId, boolean isConnected) returns error? {
    string accessToken = check getAccessToken();

    json statusData = {
        "driver_id": driverId,
        "is_connected": isConnected,
        "last_updated": time:utcNow(),
        "status": isConnected ? "connected" : "disconnected"
    };

    string path = "/driver_connections/" + driverId + "/status.json";
    http:Response response = check firebaseClient->put(path, statusData, {
        "Authorization": "Bearer " + accessToken
    });

    if response.statusCode != 200 {
        return error("Failed to update connection status: " + response.statusCode.toString());
    }
}

function storeWaypointEvent(string driverId, json data) returns error? {
    string accessToken = check getAccessToken();
    string timestamp = time:utcNow().toString();
    string path = "/ride_events/" + driverId + "/waypoints/" + timestamp + ".json";

    http:Response response = check firebaseClient->put(path, data, {
        "Authorization": "Bearer " + accessToken
    });

    if response.statusCode != 200 {
        return error("Failed to store waypoint event: " + response.statusCode.toString());
    }
}

function storeRideEvent(string driverId, string rideId, json data) returns error? {
    string accessToken = check getAccessToken();
    string timestamp = time:utcNow().toString();
    string path = "/ride_events/" + driverId + "/" + rideId + "/" + timestamp + ".json";

    http:Response response = check firebaseClient->put(path, data, {
        "Authorization": "Bearer " + accessToken
    });

    if response.statusCode != 200 {
        return error("Failed to store ride event: " + response.statusCode.toString());
    }
}


function handlePassengerConnected(websocket:Caller caller, json message) returns websocket:Error? {
    common:PassengerConnectedMessage|error parseResult = message.cloneWithType();

    if parseResult is common:PassengerConnectedMessage {
        string passengerId = parseResult.passenger_id;
        string driverId = parseResult.driver_id;

        // Check if passenger is already connected
        lock {
            if connectedPassengers.hasKey(passengerId) {
                log:printWarn("Passenger already connected: " + passengerId);
                return sendErrorResponse(caller, "Passenger already connected");
            }

            connectedPassengers[passengerId] = caller;
            passengerInfoMap[passengerId] = {
                passengerId: passengerId,
                driverId: driverId
            };
        }

        io:println("=== PASSENGER CONNECTED ===");
        io:println("Passenger ID: " + passengerId);
        io:println("Driver ID: " + driverId);
        io:println("===========================");

        // Send acknowledgment first
        json ackResponse = {
            "type": "passenger_connected_ack",
            "passenger_id": passengerId,
            "driver_id": driverId,
            "status": "success",
            "timestamp": time:utcNow()
        };

        check caller->writeMessage(ackResponse);

        // Send driver location and pickup confirmation
        check sendDriverLocationToPassenger(caller, passengerId, driverId);

        return;
    } else {
        log:printError("Invalid passenger connected message format", parseResult);
        return sendErrorResponse(caller, "Invalid passenger connected message format");
    }
}

// Function to send driver location and pickup confirmation to passenger
function sendDriverLocationToPassenger(websocket:Caller passengerCaller, string passengerId, string driverId) returns websocket:Error? {
    lock {
        // Check if driver is connected and get driver info
        if !connectedDrivers.hasKey(driverId) {
            log:printWarn("Driver not connected: " + driverId);
            json errorResponse = {
                "type": "driver_not_available",
                "message": "Driver is not currently connected",
                "driver_id": driverId,
                "timestamp": time:utcNow()
            };
            return passengerCaller->writeMessage(errorResponse);
        }

        common:DriverInfo? driverInfo = driverInfoMap[driverId];
        if driverInfo is common:DriverInfo {
            // Send driver location to passenger
            json locationResponse = {
                "type": "driver_location",
                "driver_id": driverId,
                "passenger_id": passengerId,
                "latitude": driverInfo.lastLatitude,
                "longitude": driverInfo.lastLongitude,
                "last_updated": driverInfo.lastLocationUpdate,
                "ride_id": driverInfo.rideId,
                "timestamp": time:utcNow()
            };

            check passengerCaller->writeMessage(locationResponse);

            // Send pickup confirmation
            json pickupConfirmation = {
                "type": "pickup_confirmation",
                "driver_id": driverId,
                "passenger_id": passengerId,
                "ride_id": driverInfo.rideId,
                "status": "driver_assigned",
                "message": "Your driver has been assigned and is on the way",
                "driver_location": {
                    "latitude": driverInfo.lastLatitude,
                    "longitude": driverInfo.lastLongitude,
                    "last_updated": driverInfo.lastLocationUpdate
                },
                "timestamp": time:utcNow()
            };

            check passengerCaller->writeMessage(pickupConfirmation);

            // Optionally notify the driver about passenger connection
            websocket:Caller? driverCaller = connectedDrivers[driverId];
            if driverCaller is websocket:Caller {
                json driverNotification = {
                    "type": "passenger_connected_notification",
                    "passenger_id": passengerId,
                    "driver_id": driverId,
                    "ride_id": driverInfo.rideId,
                    "message": "Passenger has connected to track your location",
                    "timestamp": time:utcNow()
                };

                var notifyResult = driverCaller->writeMessage(driverNotification);
                if notifyResult is websocket:Error {
                    log:printError("Failed to notify driver about passenger connection", notifyResult);
                }
            }

            log:printInfo("Driver location and pickup confirmation sent to passenger: " + passengerId);
        } else {
            log:printWarn("Driver info not found for driver: " + driverId);
            json errorResponse = {
                "type": "driver_info_not_available",
                "message": "Driver information not available",
                "driver_id": driverId,
                "timestamp": time:utcNow()
            };
            return passengerCaller->writeMessage(errorResponse);
        }
    }
}

// Update your handleJsonMessage function to use the enhanced location handler
// Replace the existing "location_update" case with:
// "location_update" => {
//     return handleLocationUpdateWithPassengerNotification(caller, message);
// }

// Update your onClose handler in LocationWebSocketService to handle both driver and passenger cleanup
// remote function onClose(websocket:Caller caller, int statusCode, string reason) {
//     log:printInfo(string `WebSocket connection closed. Status: ${statusCode}, Reason: ${reason}`);
//     cleanupDriverConnection(caller);
//     cleanupPassengerConnection(caller);
// }

// Update your onError handler similarly
// remote function onError(websocket:Caller caller, websocket:Error err) {
//     log:printError("WebSocket error occurred", err);
//     cleanupDriverConnection(caller);
//     cleanupPassengerConnection(caller);
// }
function handleLocationUpdateWithPassengerNotification(websocket:Caller caller, json message) returns websocket:Error? {
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
            } else {
                log:printWarn("Location update for unknown driver: " + driverId);
                return sendErrorResponse(caller, "Driver not registered");
            }
        }

        // Send location update to connected passengers for this driver
        check broadcastLocationToPassengers(driverId, parseResult);

        // Store location update in Firebase asynchronously
        json locationData = {
            "driver_id": driverId,
            "ride_id": rideId,
            "latitude": latitude,
            "longitude": longitude,
            "timestamp": parseResult.timestamp,
            "speed": parseResult.speed is decimal ? parseResult.speed : (),
            "heading": parseResult.heading is decimal ? parseResult.heading : (),
            "accuracy": parseResult.accuracy is decimal ? parseResult.accuracy : ()
        };

        var _ = start storeLocationUpdateAsync(driverId, locationData);

        json response = {
            "type": "location_received",
            "driver_id": driverId,
            "status": "success",
            "timestamp": time:utcNow()
        };

        return caller->writeMessage(response);
    } else {
        log:printError("Invalid location update message format", parseResult);
        return sendErrorResponse(caller, "Invalid location update message format");
    }
}

// Function to broadcast location updates to passengers tracking this driver
function broadcastLocationToPassengers(string driverId, common:LocationUpdateMessage locationUpdate) returns websocket:Error? {
    log:printInfo("Location update send to passenger");
    lock {
        foreach var [passengerId, passengerInfo] in passengerInfoMap.entries() {
            if passengerInfo.driverId == driverId {
                websocket:Caller? passengerCaller = connectedPassengers[passengerId];
                if passengerCaller is websocket:Caller {
                    json locationBroadcast = {
                        "type": "driver_location_update",
                        "driver_id": driverId,
                        "passenger_id": passengerId,
                        "latitude": locationUpdate.latitude,
                        "longitude": locationUpdate.longitude,
                        "speed": locationUpdate.speed,
                        "heading": locationUpdate.heading,
                        "accuracy": locationUpdate.accuracy,
                        "timestamp": locationUpdate.timestamp
                    };

                    var result = passengerCaller->writeMessage(locationBroadcast);
                    if result is websocket:Error {
                        log:printError("Failed to send location update to passenger: " + passengerId, result);
                    }
                }
            }
        }
    }
}

// Function to handle passenger disconnection
function cleanupPassengerConnection(websocket:Caller caller) {
    lock {
        string? passengerIdToRemove = ();

        // Find passenger ID by caller reference
        foreach var [key, value] in connectedPassengers.entries() {
            if value === caller {
                passengerIdToRemove = key;
                break;
            }
        }

        if passengerIdToRemove is string {
            _ = connectedPassengers.remove(passengerIdToRemove);
            common:PassengerInfo? passengerInfo = passengerInfoMap.remove(passengerIdToRemove);

            if passengerInfo is common:PassengerInfo {
                // Notify driver about passenger disconnection
                websocket:Caller? driverCaller = connectedDrivers[passengerInfo.driverId];
                if driverCaller is websocket:Caller {
                    json driverNotification = {
                        "type": "passenger_disconnected_notification",
                        "passenger_id": passengerIdToRemove,
                        "driver_id": passengerInfo.driverId,
                        "message": "Passenger has disconnected from tracking",
                        "timestamp": time:utcNow()
                    };

                    var notifyResult = driverCaller->writeMessage(driverNotification);
                    if notifyResult is websocket:Error {
                        log:printError("Failed to notify driver about passenger disconnection", notifyResult);
                    }
                }
            }

            log:printInfo("Passenger removed from connected passengers: " + passengerIdToRemove);
        }
    }
}

function notifyPassengerPickedUp(string passengerId, string driverId, string rideId, string timestamp) {
    // Check if passenger is connected via WebSocket
    websocket:Caller? passengerConnection = connectedPassengers[passengerId];
    
    if passengerConnection is websocket:Caller {
        // Get passenger info if available
        common:PassengerInfo? passengerInfo = passengerInfoMap[passengerId];
        
        json passengerMessage = {
            "type": "driver_confirmed_pickup",
            "message": "Your driver has confirmed that you have been picked up",
            "driver_id": driverId,
            "ride_id": rideId,
            "passenger_id": passengerId,
            "status": "picked_up",
            "timestamp": timestamp
        };
        
        // Send message to passenger
        var result = passengerConnection->writeMessage(passengerMessage);
        if result is websocket:Error {
            log:printError("Failed to send pickup confirmation to passenger: " + passengerId, result);
        } else {
            io:println("Pickup confirmation sent to passenger: " + " (" + passengerId + ")");
        }
    } else {
        io:println("Passenger not connected via WebSocket: " + passengerId);
        // Optional: Store the message for later delivery or send via other means (push notification, etc.)
    }
}
