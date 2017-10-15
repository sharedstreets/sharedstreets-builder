package io.sharedstreets.tools.builder.osm.inputs;


import io.sharedstreets.tools.builder.osm.model.WayEntity;
import io.sharedstreets.tools.builder.osm.parser.Parser;
import io.sharedstreets.tools.builder.osm.parser.WayParser;
import org.openstreetmap.osmosis.osmbinary.Osmformat;

public class OSMPBFWayInputFormat extends OSMPBFInputFormat<WayEntity> {

    @Override
    protected Parser createParser(Osmformat.PrimitiveBlock p) {
        return new WayParser(p);
    }

}