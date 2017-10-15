package io.sharedstreets.tools.builder.osm.model;

/**
 * Created by kpw on 5/8/17.
 */
public class NodePosition {
    public Long nodeId;
    public double lon;
    public double lat;

    public NodePosition(long nodeId, double lat, double lon) {
        this.nodeId = nodeId;
        this.lon = lon;
        this.lat = lat;
    }
}

