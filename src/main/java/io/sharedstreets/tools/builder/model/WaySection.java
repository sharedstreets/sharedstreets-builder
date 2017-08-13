package io.sharedstreets.tools.builder.model;


import io.sharedstreets.data.osm.model.NodePosition;

public class WaySection {

    public Long wayId;
    public boolean oneWay;

    public NodePosition[] nodes;
}

