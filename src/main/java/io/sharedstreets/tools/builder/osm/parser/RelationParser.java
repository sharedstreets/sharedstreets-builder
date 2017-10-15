package io.sharedstreets.tools.builder.osm.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.openstreetmap.osmosis.osmbinary.Osmformat;

import io.sharedstreets.tools.builder.osm.OSMContext;
import io.sharedstreets.tools.builder.osm.model.RelatedObject;
import io.sharedstreets.tools.builder.osm.model.Relation;

public class RelationParser extends Parser<Relation> {

    private OSMContext ctx;

    private List<Osmformat.PrimitiveGroup> groups;

    public RelationParser(Osmformat.PrimitiveBlock block) {
        assert block != null;
        this.ctx = createOSMContext(block);
        groups = new ArrayList<Osmformat.PrimitiveGroup>(block.getPrimitivegroupList());
    }

    public class RelationParserDecomposer {

        private ArrayList<Osmformat.Relation> left;

        public RelationParserDecomposer(List<Osmformat.Relation> rels) {
            assert rels != null;
            left = new ArrayList<Osmformat.Relation>(rels);
        }

        public Relation next() {

            if (left.size() == 0)
                return null;

            Osmformat.Relation r = left.get(0);
            left.remove(0);

            // handling fields

            long id = r.getId();

            HashMap<String, String> flds = null;
            for (int i = 0; i < r.getKeysCount(); i++) {
                String k = ctx.getStringById(r.getKeys(i));
                String v = ctx.getStringById(r.getVals(i));
                if (flds == null) {
                    flds = new HashMap<String, String>();
                }
                flds.put(k, v);
            }

            // ok we have fields

            // extract outer and inner ways relations

            long rid = 0;
            List<RelatedObject> relatedObjects = null;
            for (int i = 0; i < r.getMemidsCount(); i++) {
                rid += r.getMemids(i);
                String role = ctx.getStringById(r.getRolesSid(i));
                Osmformat.Relation.MemberType mt = r.getTypes(i);

                String stringType = null;
                switch (mt) {

                    case NODE:
                        stringType = "node";
                        break;
                    case WAY:
                        stringType = "way";
                        break;
                    case RELATION:
                        stringType = "relation";
                        break;
                    default:
                        String msg = "unknown relation type for object " + id + " and relation " + rid;

                        throw new RuntimeException(msg);
                }

                if (relatedObjects == null) {
                    relatedObjects = new ArrayList<>();
                }

                RelatedObject ro = new RelatedObject();
                ro.relatedId = rid;
                ro.role = role;
                ro.type = stringType;

                // add relation
                relatedObjects.add(ro);

            }

            long wid = r.getId();

            Relation returnValue = new Relation();
            returnValue.fields = flds;
            returnValue.id = wid;
            returnValue.relatedObjects = (relatedObjects == null ? null
                    : relatedObjects.toArray(new RelatedObject[relatedObjects.size()]));

            return returnValue;
        }

    }

    private Osmformat.PrimitiveGroup current;

    private RelationParserDecomposer decomposer;

    @Override
    public Relation next() throws Exception {

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
            decomposer = new RelationParserDecomposer(g.getRelationsList());
            groups.remove(0);

        }

        assert decomposer != null;

        Relation w = decomposer.next();

        if (w == null) {
            // next group
            current = null;
            return next();
        }

        return w;
    }

}