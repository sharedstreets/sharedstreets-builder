package io.sharedstreets.tools.builder.osm.inputs;


import io.sharedstreets.tools.builder.osm.model.NodeEntity;
import io.sharedstreets.tools.builder.osm.parser.NodeParser;
import io.sharedstreets.tools.builder.osm.parser.Parser;
import org.openstreetmap.osmosis.osmbinary.Osmformat;

public class OSMPBFNodeInputFormat extends OSMPBFInputFormat<NodeEntity> {

    @Override
    protected Parser createParser(Osmformat.PrimitiveBlock p) {
        return new NodeParser(p);
    }

}