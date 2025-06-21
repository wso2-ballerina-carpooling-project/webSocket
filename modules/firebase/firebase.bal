import lakpahana/firebase_realtime_database;

public function firebase() returns firebase_realtime_database:FirebaseDatabaseClient|error {
    json firebaseConfigJson = {
        apiKey: "AIzaSyC7e4X0Rf-Ws03NBCiDMZx-evHO3-gfPZ4",
        authDomain: "carpooling-c6aa5.firebaseapp.com", // Fixed format
        databaseURL: "https://carpooling-c6aa5-default-rtdb.firebaseio.com",
        projectId: "carpooling-c6aa5",
        storageBucket: "carpooling-c6aa5.appspot.com", // Fixed format
        messagingSenderId: "992894789225",
        appId: "1:992894789225:web:e86684610562e8163de4f8",
        measurementId: "G-0VY7MB9KJK"
    };
    
    // Correct syntax for creating the client
    firebase_realtime_database:FirebaseDatabaseClient firebaseClient = 
        check new (firebaseConfigJson, "../../serviceAccount.json");
    
    return firebaseClient;
}