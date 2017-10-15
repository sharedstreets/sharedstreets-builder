package io.sharedstreets.tools.builder.osm.model;

/**
 * Created by kpw on 5/8/17.
 */
public class WayNodeLink {
    public long wayId;
    public long nodeId;
    public int  order;
    public boolean terminatingNode;

    public WayNodeLink(long wayId, long nodeId, int  order, boolean terminatingNode){
        this.wayId = wayId;
        this.nodeId = nodeId;
        this.order = order;
        this.terminatingNode = terminatingNode;
    }
}
