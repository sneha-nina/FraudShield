package com.example.fraud_detection_system.rules;

import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class LocationRule {

    // Coordinates for test cities {city: {lat, lon}}
    private static final Map<String, double[]> CITY_COORDINATES = Map.of(
            "Delhi",    new double[]{28.6139, 77.2090},
            "Mumbai",   new double[]{19.0760, 72.8777},
            "Dubai",    new double[]{25.2048, 55.2708},
            "London",   new double[]{51.5074, -0.1278},
            "NewYork",  new double[]{40.7128, -74.0060},
            "Singapore",new double[]{1.3521, 103.8198}
    );

    // Max speed in km/h considered physically possible (commercial flight)
    private static final double MAX_POSSIBLE_SPEED_KMH = 900.0;

    public boolean isImpossibleTravel(String lastLocation, String currentLocation,
                                      long lastTimestamp, long currentTimestamp) {
        double[] lastCoords = CITY_COORDINATES.get(lastLocation);
        double[] currentCoords = CITY_COORDINATES.get(currentLocation);

        // If either city is unknown, skip the check
        if (lastCoords == null || currentCoords == null) return false;

        // Same location — not suspicious
        if (lastLocation.equals(currentLocation)) return false;

        double distanceKm = haversineDistance(
                lastCoords[0], lastCoords[1],
                currentCoords[0], currentCoords[1]);

        double timeHours = (currentTimestamp - lastTimestamp) / 3600000.0;

        // Avoid division by zero
        if (timeHours <= 0) return true;

        double speed = distanceKm / timeHours;

        return speed > MAX_POSSIBLE_SPEED_KMH;
    }

    private double haversineDistance(double lat1, double lon1,
                                     double lat2, double lon2) {
        final int EARTH_RADIUS_KM = 6371;

        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);

        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return EARTH_RADIUS_KM * c;
    }
}
