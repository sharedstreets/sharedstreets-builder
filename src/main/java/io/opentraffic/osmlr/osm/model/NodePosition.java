package io.opentraffic.osmlr.osm.model;

/**
 * Created by kpw on 5/8/17.
 */
public class NodePosition {
    public long nodeId;
    public double x;
    public double y;

    public NodePosition(long nodeId, double x, double y) {
        this.nodeId = nodeId;
        this.x = x;
        this.y = y;
    }
}

