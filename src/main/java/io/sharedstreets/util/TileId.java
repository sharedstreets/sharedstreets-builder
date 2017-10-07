package io.sharedstreets.util;


import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import java.io.IOException;

public class TileId implements Value, Comparable<TileId> {

    private static final int ZOOM_LEVEL = 10;
    private static final int TILE_SIZE = 256;
    private static final double MIN_LAT = -85.05112878;
    private static final double MAX_LAT = 85.05112878;
    private static final double MIN_LON = -180;
    private static final double MAX_LON = 180;
    private static int mMaxZoomLevel = 22;

    public int x;
    public int y;

    public static TileId lonLatToTileId(double lon, double lat) {

        lat = clip(lat, MIN_LAT, MAX_LAT);
        lon = clip(lon, MIN_LON, MAX_LON);

        final double x = (lon + 180) / 360;
        final double sinLatitude = Math.sin(lat * Math.PI / 180);
        final double y = 0.5 - Math.log((1 + sinLatitude) / (1 - sinLatitude)) / (4 * Math.PI);

        final int mapSize = TILE_SIZE << (ZOOM_LEVEL < mMaxZoomLevel ? ZOOM_LEVEL : mMaxZoomLevel);

        int px = (int) clip(x * mapSize + 0.5, 0, mapSize - 1);
        int py = (int) clip(y * mapSize + 0.5, 0, mapSize - 1);

        TileId tileId = new TileId();
        tileId.x = px / TILE_SIZE;
        tileId.y = py / TILE_SIZE;

        return tileId;
    }

    private static double clip(final double n, final double minValue, final double maxValue) {
        return Math.min(Math.max(n, minValue), maxValue);
    }

    public String toString() {
        return "10-" + x + "-" + y;
    }

    @Override
    public int compareTo(TileId o) {
        if(this.x == o.x) {
            if(this.y == o.y)
                return 0;
            else if(this.y < o.y) {
                return 1;
            }
            else
                return -1;
        }
        else {
            if(this.x < o.x)
                return 1;
            else
                return -1;
        }
    }


    public void write(DataOutputView out) throws IOException {
        out.write(x);
        out.write(y);
    }

    @Override
    public void read(DataInputView in) throws IOException {
        this.x = in.readInt();
        this.y = in.readInt();
    }
}
