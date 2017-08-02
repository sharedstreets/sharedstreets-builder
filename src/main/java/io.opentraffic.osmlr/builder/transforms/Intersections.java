package io.opentraffic.osmlr.builder.transforms;


import io.opentraffic.osmlr.builder.model.Intersection;
import io.opentraffic.osmlr.osm.OSMDataStream;
import io.opentraffic.osmlr.osm.model.WayNodeLink;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.util.Iterator;



public class Intersections {

    public enum IntersectionType {
        SPLITTING,
        JOINING
    }

    class IntersectionReducer
            implements GroupReduceFunction<WayNodeLink, Intersection> {

        @Override
        public void reduce(Iterable<WayNodeLink> in, Collector<Intersection> out) {


            Intersection intersection = new Intersection();

            // add all strings of the group to the set
            for (WayNodeLink n : in) {
                intersection.addWay(n.wayId, n.nodeId, n.terminatingNode);
            }

            if(intersection.isIntersection())
                out.collect(intersection);
            }
    }

    // intersections
    public DataSet<Intersection> intersections;

    public Intersections(OSMDataStream dataStream) {

        // group by node_id reduce nodes to intersections with > 1 ways
        // using intersections with way count > 1 to merge ways into OSMLR segments
        // using intersections with way count > 2 to split ways

        intersections = dataStream.orderedWayNodeLink.groupBy(new KeySelector<WayNodeLink, Long>() {
            @Override
            public Long getKey(WayNodeLink link) {
                return link.wayId;
            }
        }).reduceGroup(new IntersectionReducer());
    }

    public DataSet<Intersection> splittingIntersections(){

        return intersections.filter(new FilterFunction<Intersection>() {
            @Override
            public boolean filter(Intersection value) throws Exception {
                return value.isSplitting();
            }
        });
    }

    public DataSet<Intersection> mergingIntersections(){

        return intersections.filter(new FilterFunction<Intersection>() {
            @Override
            public boolean filter(Intersection value) throws Exception {
                return value.isMerging();
            }
        });
    }

}
