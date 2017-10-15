package io.sharedstreets.tools.builder.osm.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.sharedstreets.tools.builder.osm.OSMContext;
import io.sharedstreets.tools.builder.osm.model.RelatedObject;
import io.sharedstreets.tools.builder.osm.model.WayEntity;
import org.openstreetmap.osmosis.osmbinary.Osmformat;

public class WayParser extends Parser<WayEntity> {

    private OSMContext ctx;
    private List<Osmformat.PrimitiveGroup> groups;

    public WayParser(Osmformat.PrimitiveBlock block) {

        assert block != null;

        this.ctx = createOSMContext(block);

        groups = new ArrayList<Osmformat.PrimitiveGroup>(block.getPrimitivegroupList());

    }

    public class WayParserDecomposer {

        private ArrayList<Osmformat.Way> left;

        public WayParserDecomposer(List<Osmformat.Way> ways) {
            assert ways != null;
            left = new ArrayList<Osmformat.Way>(ways);
        }

        public WayEntity next() {

            if (left.size() == 0)
                return null;

            Osmformat.Way w = left.get(0);
            left.remove(0);

            long lastRef = 0;
            List<Long> l = w.getRefsList();

            long[] refids = new long[l.size()];
            // liste des references ...

            List<RelatedObject> rels = null;

            int cpt = 0;
            for (Long theid : l) {
                if (rels == null)
                    rels = new ArrayList<RelatedObject>();
                lastRef += theid;
                refids[cpt++] = lastRef;
                RelatedObject relo = new RelatedObject();
                relo.relatedId = lastRef;
                rels.add(relo);
            }

            Map<String, String> flds = null;

            for (int i = 0; i < w.getKeysCount(); i++) {
                String k = ctx.getStringById(w.getKeys(i));
                String v = ctx.getStringById(w.getVals(i));
                if (flds == null) {
                    flds = new HashMap<String, String>();
                }
                flds.put(k, v);
            }

            long wid = w.getId();

            WayEntity r = new WayEntity();

            r.fields = flds;
            r.id = wid;
            r.relatedObjects = (rels == null ? null : rels.toArray(new RelatedObject[rels.size()]));

            return r;
        }

    }

    private Osmformat.PrimitiveGroup current;

    private WayParserDecomposer decomposer;

    @Override
    public WayEntity next() throws Exception {

        if (groups == null) { // end of group read
            return null;
        }

        // next primitive group
        if (current == null) {

            if (groups.size() == 0) {
                groups = null;
                return null;
            }

            assert groups.size() > 0;
            Osmformat.PrimitiveGroup g = groups.get(0);
            current = g;
            decomposer = new WayParserDecomposer(g.getWaysList());
            groups.remove(0);

        }

        assert decomposer != null;

        WayEntity w = decomposer.next();

        if (w == null) {
            // next group
            current = null;
            return next();
        }

        return w;
    }

}