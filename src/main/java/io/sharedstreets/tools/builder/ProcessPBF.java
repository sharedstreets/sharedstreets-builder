package io.sharedstreets.tools.builder;

import io.sharedstreets.tools.builder.transforms.Intersections;
import io.sharedstreets.data.osm.OSMDataStream;
import io.sharedstreets.tools.builder.transforms.Segments;
import org.apache.flink.api.java.ExecutionEnvironment;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.net.URL;


public class ProcessPBF {

    static Logger LOG = LoggerFactory.getLogger(ProcessPBF.class);

    public static boolean DEBUG_OUTPUT = true;

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        String inputFile = "data/cebu_extract.pbf";

        OSMDataStream dataStream = new OSMDataStream(inputFile, env);

        // create intersections
        Intersections intersections = new Intersections(dataStream);


        try {
            long count = intersections.intersections.count();
            LOG.info("Intersections count: {}", count);
        } catch (Exception e){
            e.printStackTrace();
        }


        // split ways
        Segments segments = new Segments(dataStream, intersections);

        long wayCount = dataStream.ways.count();

        LOG.info("Way count: {}", wayCount);


        long segmentCount = segments.segements.count();
        LOG.info("BaseSegment count: {}", segmentCount);

//        DataSet<NodeEntity> intersectionNodes = dataStream
//                .nodes.joinWithHuge(splittingIntersections)
//                .where(new KeySelector<NodeEntity, Long>() {
//                    @Override
//                    public Long getKey(NodeEntity value) throws Exception {
//                        return value.id;
//                    }
//                }).equalTo(new KeySelector<Intersection, Long>() {
//                    @Override
//                    public Long getKey(Intersection value) throws Exception {
//                        return value.nodeId;
//                    }
//                })
//                .map(new MapFunction<Tuple2<NodeEntity, Intersection>, NodeEntity>() {
//                    @Override
//                    public NodeEntity map(Tuple2<NodeEntity, Intersection> in) throws Exception {
//                        return in.f0;
//
//                    }
//                });

//        try {
//            long count = intersectionNodes.count();
//            System.out.printf("Intersection nodes count: %d", count);
//        } catch (Exception e){
//            e.printStackTrace();
//        }
//
//
//        intersectionNodes.write(new GeoJSONOutputFormat(), "/tmp/splitting_intersections.geojson", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute();

    }

}


