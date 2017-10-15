package io.sharedstreets.tools.builder;

import io.sharedstreets.data.SharedStreetsGeometry;
import io.sharedstreets.data.SharedStreetsIntersection;
import io.sharedstreets.data.outputs.SharedStreetsGeometryGeoJSONTileFormat;
import io.sharedstreets.data.outputs.SharedStreetsIntersectionGeoJSONTileFormat;
import io.sharedstreets.tools.builder.transforms.Intersections;
import io.sharedstreets.tools.builder.osm.OSMDataStream;
import io.sharedstreets.tools.builder.transforms.BaseSegments;
import io.sharedstreets.tools.builder.transforms.SharedStreets;
import org.apache.flink.api.java.ExecutionEnvironment;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


public class ProcessPBF {

    static Logger LOG = LoggerFactory.getLogger(ProcessPBF.class);

    public static boolean DEBUG_OUTPUT = true;

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String inputFile = "data/nyc_extract.pbf";

        OSMDataStream dataStream = new OSMDataStream(inputFile, env);

        // create intersections
        Intersections intersections = new Intersections(dataStream);

        // split ways
        BaseSegments segments = new BaseSegments(dataStream, intersections);

        SharedStreets streets = new SharedStreets(segments);

        streets.geometries.output(new SharedStreetsGeometryGeoJSONTileFormat<SharedStreetsGeometry>("/tmp/tiles/")).setParallelism(1);
        streets.intersections.output(new SharedStreetsIntersectionGeoJSONTileFormat<SharedStreetsIntersection>("/tmp/tiles/")).setParallelism(1);

        env.execute();

    }

}


