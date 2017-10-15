package io.sharedstreets.tools.builder.osm.parser;

import org.openstreetmap.osmosis.osmbinary.Osmformat;

import io.sharedstreets.tools.builder.osm.OSMContext;
import io.sharedstreets.tools.builder.osm.model.AttributedEntity;

public abstract class Parser<T extends AttributedEntity> {

    /**
     * retrieve the next entity
     *
     * @return
     * @throws Exception
     */
    public abstract T next() throws Exception;

    protected OSMContext createOSMContext(Osmformat.PrimitiveBlock block) {
        assert block != null;
        Osmformat.StringTable stablemessage = block.getStringtable();
        String[] strings = new String[stablemessage.getSCount()];

        for (int i = 0; i < strings.length; i++) {
            strings[i] = stablemessage.getS(i).toStringUtf8();
        }

        int granularity = block.getGranularity();
        long lat_offset = block.getLatOffset();
        long lon_offset = block.getLonOffset();
        int date_granularity = block.getDateGranularity();

        return new OSMContext(granularity, lat_offset, lon_offset, date_granularity, strings);
    }
}
