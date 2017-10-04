package io.sharedstreets.util;


import io.sharedstreets.data.osm.model.Point;

public class Haversine {
    private static final int EARTH_RADIUS = 6371009; // meters

    // haversine distance in meters

    public static double distance(Point start, Point end){
        return distance(start.lat, start.lon, end.lat, end.lon);
    }

    public static double distance(double startLat, double startLong,
                                  double endLat, double endLong) {

        double deltaLat  = Math.toRadians((endLat - startLat));
        double deltaLong = Math.toRadians((endLong - startLong));

        startLat = Math.toRadians(startLat);
        endLat   = Math.toRadians(endLat);

        double a = haversine(deltaLat) + Math.cos(startLat) * Math.cos(endLat) * haversine(deltaLong);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return EARTH_RADIUS * c;
    }

    public static double haversine(double val) {
        return Math.pow(Math.sin(val / 2), 2);
    }
}