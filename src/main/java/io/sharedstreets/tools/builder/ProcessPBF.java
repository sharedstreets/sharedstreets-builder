package io.sharedstreets.tools.builder;

import io.sharedstreets.data.SharedStreetGeometry;
import io.sharedstreets.tools.builder.outputs.GeoJSONOutputFormat;
import io.sharedstreets.tools.builder.outputs.tiles.SSGGeoJSONTile;
import io.sharedstreets.tools.builder.outputs.tiles.TileOutputFormat;
import io.sharedstreets.tools.builder.transforms.Intersections;
import io.sharedstreets.data.osm.OSMDataStream;
import io.sharedstreets.tools.builder.transforms.BaseSegments;
import io.sharedstreets.tools.builder.transforms.SharedStreets;
import org.apache.flink.api.java.ExecutionEnvironment;

import org.apache.flink.core.fs.FileSystem;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.IOException;


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

        SharedStreets references = new SharedStreets(segments);

        references.geometries.output(new SSGGeoJSONTile<SharedStreetGeometry>("/tmp/tiles")).setParallelism(1);

        env.execute();

    }

}


