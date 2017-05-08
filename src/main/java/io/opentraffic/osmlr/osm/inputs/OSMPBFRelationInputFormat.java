package io.opentraffic.osmlr.osm.inputs;

import crosby.binary.Osmformat.PrimitiveBlock;
import io.opentraffic.osmlr.osm.model.Relation;
import io.opentraffic.osmlr.osm.parser.Parser;
import io.opentraffic.osmlr.osm.parser.RelationParser;



public class OSMPBFRelationInputFormat extends OSMPBFInputFormat<Relation> {

    @Override
    protected Parser createParser(PrimitiveBlock p) {
        return new RelationParser(p);
    }

}