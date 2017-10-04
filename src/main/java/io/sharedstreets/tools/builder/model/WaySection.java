package io.sharedstreets.tools.builder.model;


import io.sharedstreets.data.osm.model.NodePosition;
import io.sharedstreets.data.osm.model.Way;

public class WaySection {

    public Long wayId;
    public boolean oneWay;
    public boolean roundabout;
    public boolean link;
    public Way.ROAD_CLASS roadClass;

    public NodePosition[] nodes;

}

