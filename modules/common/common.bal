public type DriverInfo record {
    string driverId;
    string rideId;
    string connectionTime;
    decimal? lastLatitude;
    decimal? lastLongitude;
    string? lastLocationUpdate;
};

// Message types from Flutter app
public type LocationUpdateMessage record {
    string 'type;
    string driver_id;
    string ride_id;
    decimal latitude;
    decimal longitude;
    decimal speed?;
    decimal heading?;
    string timestamp;
    decimal accuracy?;
};

public type DriverConnectedMessage record {
    string 'type;
    string driver_id;
    string ride_id;
    string timestamp;
};

public type DriverDisconnectedMessage record {
    string 'type;
    string driver_id;
    string ride_id;
    string timestamp;
};

public type HeartbeatMessage record {
    string 'type;
    string driver_id;
    string timestamp;
};

public type WaypointApproachingMessage record {
    string 'type;
    string driver_id;
    string ride_id;
    decimal waypoint_latitude;
    decimal waypoint_longitude;
    decimal distance_to_waypoint;
    string timestamp;
};

public type PickupArrivalMessage record {
    string 'type;
    string driver_id;
    string ride_id;
    string passenger_name;
    string timestamp;
};

public type PassengerPickedUpMessage record {
    string 'type;
    string driver_id;
    string ride_id;
    string passenger_name;
    string timestamp;
};
