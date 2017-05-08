package io.opentraffic.osmlr.osm.inputs;


import crosby.binary.Osmformat.PrimitiveBlock;

import io.opentraffic.osmlr.osm.model.WayEntity;
import io.opentraffic.osmlr.osm.parser.Parser;
import io.opentraffic.osmlr.osm.parser.WayParser;

public class OSMPBFWayInputFormat extends OSMPBFInputFormat<WayEntity> {

    @Override
    protected Parser createParser(PrimitiveBlock p) {
        return new WayParser(p);
    }

}