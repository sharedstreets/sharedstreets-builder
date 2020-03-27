package io.sharedstreets.tools.builder.model;


import io.sharedstreets.tools.builder.osm.model.NodePosition;
import io.sharedstreets.tools.builder.osm.model.Way;

import java.util.Map;

public class WaySection {

    public Long wayId;
    public String name;
    public boolean oneWay;
    public boolean roundabout;
    public boolean link;
    public Way.ROAD_CLASS roadClass;

    public NodePosition[] nodes;

    public Map<String, String> fields;

}

