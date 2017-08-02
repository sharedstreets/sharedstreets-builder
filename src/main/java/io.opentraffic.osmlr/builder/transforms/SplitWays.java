package io.opentraffic.osmlr.builder.transforms;


import io.opentraffic.osmlr.builder.model.Intersection;
import io.opentraffic.osmlr.osm.OSMDataStream;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class SplitWays {

    public SplitWays(OSMDataStream dataStream, Intersections intersections) {

        DataSet<Intersection> splittingIntersections = intersections.splittingIntersections();

        splittingIntersections.flatMap(new FlatMapFunction<Intersection, Tuple2<Long, Long>>() {
            public void flatMap(Intersection value, Collector<Tuple2<Long, Long>> out) {
                for (Long wayId : value.intersectingWays) {
                    out.collect(new Tuple2<Long, Long>(wayId, value.nodeId));
                }
            }
        });
    }
}
