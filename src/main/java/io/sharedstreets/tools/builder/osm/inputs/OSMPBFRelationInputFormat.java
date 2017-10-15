package io.sharedstreets.tools.builder.osm.inputs;


import io.sharedstreets.tools.builder.osm.model.Relation;
import io.sharedstreets.tools.builder.osm.parser.Parser;
import io.sharedstreets.tools.builder.osm.parser.RelationParser;
import org.openstreetmap.osmosis.osmbinary.Osmformat;


public class OSMPBFRelationInputFormat extends OSMPBFInputFormat<Relation> {

    @Override
    protected Parser createParser(Osmformat.PrimitiveBlock p) {
        return new RelationParser(p);
    }

}