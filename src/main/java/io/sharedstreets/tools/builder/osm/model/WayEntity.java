package io.sharedstreets.tools.builder.osm.model;

public class WayEntity extends Relation {

    public boolean isHighway() {

        if (fields == null)
            return false;

        assert fields != null;

        return fields.containsKey("highway");

    }
}