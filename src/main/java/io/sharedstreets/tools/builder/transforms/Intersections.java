package io.sharedstreets.tools.builder.transforms;


import io.sharedstreets.tools.builder.model.WayIntersection;
import io.sharedstreets.tools.builder.osm.OSMDataStream;
import io.sharedstreets.tools.builder.osm.model.WayNodeLink;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.Collector;


public class Intersections {

    class IntersectionReducer
            implements GroupReduceFunction<WayNodeLink, WayIntersection> {

        @Override
        public void reduce(Iterable<WayNodeLink> in, Collector<WayIntersection> out) {

            WayIntersection intersection = new WayIntersection();

            for (WayNodeLink n : in) {
                intersection.addWay(n.wayId, n.nodeId, n.terminatingNode);
            }

            if(intersection.isIntersection())
                out.collect(intersection);
            }
    }

    // intersections
    public DataSet<WayIntersection> intersections;

    public Intersections(OSMDataStream dataStream) {

        // group by node_id reduce nodes to intersections with > 1 ways
        // using intersections with way count > 1 to merge ways into OSMLR segments
        // using intersections with way count > 2 to split ways

        intersections = dataStream.orderedWayNodeLink.groupBy(new KeySelector<WayNodeLink, Long>() {
            @Override
            public Long getKey(WayNodeLink link) {
                return link.nodeId;
            }
        }).reduceGroup(new IntersectionReducer());
    }

    public DataSet<WayIntersection> splittingIntersections(){

        return intersections.filter(new FilterFunction<WayIntersection>() {
            @Override
            public boolean filter(WayIntersection value) throws Exception {
                return value.isSplitting();
            }
        });
    }

    public DataSet<WayIntersection> mergingIntersections(){

        return intersections.filter(new FilterFunction<WayIntersection>() {
            @Override
            public boolean filter(WayIntersection value) throws Exception {
                return value.isMerging();
            }
        });
    }

}
