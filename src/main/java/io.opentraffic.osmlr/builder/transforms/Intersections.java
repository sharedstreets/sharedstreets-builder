package io.opentraffic.osmlr.builder.transforms;


import io.opentraffic.osmlr.builder.model.Intersection;
import io.opentraffic.osmlr.osm.OSMDataStream;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.util.Iterator;



public class Intersections {

    public enum IntersectionType {
        SPLITTING,
        JOINING
    }

    class IntersectionReducer
            implements GroupReduceFunction<Tuple4<Long, Long, Integer, Boolean>, Intersection> {

        @Override
        public void reduce(Iterable<Tuple4<Long, Long, Integer, Boolean>> in, Collector<Intersection> out) {


            Intersection intersection = new Intersection();

            // add all strings of the group to the set
            for (Tuple4<Long, Long, Integer, Boolean> n : in) {
                intersection.addWay(n.f0, n.f1, n.f3);
            }

            if(intersection.isIntersection())
                out.collect(intersection);
        }
    }

    // intersections
    // node_id, Set<way_id>
    public DataSet<Intersection> intersections;

    public Intersections(OSMDataStream dataStream) {

        // group by node_id reduce nodes to intersections with > 1 ways
        // using intersections with way count > 1 to merge ways into OSMLR segments
        // using intersections with way count > 2 to split ways


        intersections = dataStream.orderedWayNodeLink.groupBy(1).reduceGroup(new IntersectionReducer());
    }
}
