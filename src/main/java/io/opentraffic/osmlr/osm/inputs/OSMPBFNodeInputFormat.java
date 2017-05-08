package io.opentraffic.osmlr.osm.inputs;

import crosby.binary.Osmformat.PrimitiveBlock;

import io.opentraffic.osmlr.osm.model.NodeEntity;
import io.opentraffic.osmlr.osm.parser.NodeParser;
import io.opentraffic.osmlr.osm.parser.Parser;

public class OSMPBFNodeInputFormat extends OSMPBFInputFormat<NodeEntity> {

    @Override
    protected Parser createParser(PrimitiveBlock p) {
        return new NodeParser(p);
    }

}