package io.opentraffic.osmlr.builder;

import io.opentraffic.osmlr.builder.model.Intersection;
import io.opentraffic.osmlr.builder.outputs.GeoJSONOutputFormat;
import io.opentraffic.osmlr.builder.transforms.Intersections;
import io.opentraffic.osmlr.osm.OSMDataStream;
import io.opentraffic.osmlr.osm.inputs.OSMPBFNodeInputFormat;
import io.opentraffic.osmlr.osm.model.NodeEntity;
import io.opentraffic.osmlr.osm.tools.MapStringTools;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.core.fs.FileSystem;


public class ProcessPBF {

    private static String quoteString(String s) {

        if (s == null)
            return null;

        // remove quotes
        return "\"" + s.replaceAll("\"", "") + "\"";
    }

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        String inputFile = "data/cebu_extract.pbf";


        OSMDataStream dataStream = new OSMDataStream(inputFile, env);

        Intersections intersections = new Intersections(dataStream);


        try {
            long count = intersections.intersections.count();
            System.out.printf("Intersections count: %d", count);
        } catch (Exception e){
            e.printStackTrace();
        }

        DataSet<Intersection> splittingIntersections = intersections
                .intersections.filter(new FilterFunction<Intersection>() {
                    @Override
                    public boolean filter(Intersection value) throws Exception {
                        return value.isSplitting();
                    }
                }
            );

        DataSet<NodeEntity> intersectionNodes = dataStream
                .nodes.joinWithHuge(splittingIntersections)
                .where(new KeySelector<NodeEntity, Long>() {
                    @Override
                    public Long getKey(NodeEntity value) throws Exception {
                        return value.id;
                    }
                }).equalTo(new KeySelector<Intersection, Long>() {
                    @Override
                    public Long getKey(Intersection value) throws Exception {
                        return value.nodeId;
                    }
                })
                .map(new MapFunction<Tuple2<NodeEntity, Intersection>, NodeEntity>() {
                    @Override
                    public NodeEntity map(Tuple2<NodeEntity, Intersection> in) throws Exception {
                        return in.f0;

                    }
                });

        try {
            long count = intersectionNodes.count();
            System.out.printf("Intersection nodes count: %d", count);
        } catch (Exception e){
            e.printStackTrace();
        }

        /*intersectionNodes.map(new MapFunction<NodeEntity, Tuple4<Long, Double, Double, String>>() {
            @Override
            public Tuple4<Long, Double, Double, String> map(NodeEntity value) throws Exception {
                return new Tuple4<>(value.id, value.x, value.y,
                        quoteString(MapStringTools.convertToString(value.fields)));
            }
        }).writeAsCsv("/tmp/intersection_nodes.csv").setParallelism(1);
        */



        intersectionNodes.write(new GeoJSONOutputFormat(), "/tmp/splitting_intersections.geojson").setParallelism(1);

        env.execute();

    }

}


