package io.sharedstreets.tools.builder.osm.parser;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import io.sharedstreets.tools.builder.osm.OSMContext;
import io.sharedstreets.tools.builder.osm.model.NodeEntity;
import org.openstreetmap.osmosis.osmbinary.Osmformat;

/**
 * this object parse the nodes, and their associated elements
 *
 * @author pfreydiere
 *
 */
public class NodeParser extends Parser<NodeEntity> {

    private Osmformat.PrimitiveGroup current;

    public abstract class State {
        public abstract NodeEntity next();
    }

    public class DenseNodeState extends State {
        private int i = 0;

        long lastId = 0;
        long lastLat = 0;
        long lastLon = 0;
        int j = 0;

        @Override
        public NodeEntity next() {
            Osmformat.DenseNodes denseNodes = current.getDense();
            if (i >= denseNodes.getIdCount())
                return null;

            lastId += denseNodes.getId(i);
            lastLat += denseNodes.getLat(i);
            lastLon += denseNodes.getLon(i);

            Map<String, String> flds = null;

            try {
                if (denseNodes.getKeysValsCount() > 0) {
                    while (denseNodes.getKeysVals(j) != 0) {
                        int keyid = denseNodes.getKeysVals(j++);
                        int valid = denseNodes.getKeysVals(j++);
                        String k = "";
                        if (keyid < ctx.getStringLength())
                            k = ctx.getStringById(keyid);
                        else
                            System.out.println("error in encoding");

                        String v = "";
                        if (valid < ctx.getStringLength())
                            v = ctx.getStringById(valid);
                        else
                            System.out.println("error in encoding");

                        if (flds == null) {
                            flds = new HashMap<String, String>();
                        }

                        flds.put(k, v);

                        // System.out.println(k + "->" + v);

                    }
                    j++; // Skip over the '0' delimiter.
                }
            } catch (Exception ex) {
                System.out.println("error in creating dense points : " + ex.getMessage());
                ex.printStackTrace(System.err);
            }

            NodeEntity ne = new NodeEntity();
            ne.fields = flds;
            ne.id = lastId;
            ne.x = ctx.parseLon(lastLon);
            ne.y = ctx.parseLat(lastLat);
            i++;
            return ne;
        }

    }

    public class NodesState extends State {

        private int currentNode = 0;

        public NodesState() {

        }

        @Override
        public NodeEntity next() {

            if (current.getNodesCount() > currentNode) {
                Osmformat.Node n = current.getNodes(currentNode);
                currentNode++;

                return constructNode(n);
            }

            return null;
        }

        private NodeEntity constructNode(Osmformat.Node n) {
            Map<String, String> fields = null;

            for (int i = 0; i < n.getKeysCount(); i++) {
                int keys = n.getKeys(i);
                String k = ctx.getStringById(keys);
                String v = ctx.getStringById(n.getVals(i));
                if (fields == null) {
                    fields = new HashMap<String, String>();
                }
                fields.put(k, v);
            }

            NodeEntity ne = new NodeEntity();
            ne.fields = fields;
            ne.id = n.getId();
            ne.x = ctx.parseLon(n.getLon());
            ne.y = ctx.parseLat(n.getLat());
            return ne;
        }

    }

    private List<Osmformat.PrimitiveGroup> groups;
    private OSMContext ctx;

    public NodeParser(Osmformat.PrimitiveBlock block) {
        assert block != null;

        this.ctx = createOSMContext(block);

        groups = new ArrayList<Osmformat.PrimitiveGroup>(block.getPrimitivegroupList());

    }

    private State currentState = null;

    public NodeEntity next() throws Exception {

        try {

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

                currentState = new NodesState();
                groups.remove(0);

            }

            assert currentState != null;

            NodeEntity n = currentState.next();
            if (n == null) {
                if (currentState instanceof NodesState) {
                    // next, switching to denseNodeState
                    currentState = new DenseNodeState();
                    return next();
                } else {
                    // next group
                    current = null;
                    return next();
                }
            }

            assert n != null;
            return n;
        } catch (Exception ex) {
            System.out.println(" error parsing entity :" + ex.getMessage());
            throw ex;
        }
    }

}