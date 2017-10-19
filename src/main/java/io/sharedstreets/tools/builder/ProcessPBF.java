package io.sharedstreets.tools.builder;

import com.jsoniter.spi.JsoniterSpi;
import io.sharedstreets.data.SharedStreetsGeometry;
import io.sharedstreets.data.SharedStreetsIntersection;
import io.sharedstreets.data.SharedStreetsOSMMetadata;
import io.sharedstreets.data.SharedStreetsReference;
import io.sharedstreets.data.outputs.SharedStreetsGeometryGeoJSONTileFormat;
import io.sharedstreets.data.outputs.SharedStreetsIntersectionGeoJSONTileFormat;
import io.sharedstreets.data.outputs.SharedStreetsOSMMetadataJSONEncoder;
import io.sharedstreets.data.outputs.SharedStreetsReferenceJSONEncoder;
import io.sharedstreets.tools.builder.tiles.JSONTileFormat;
import io.sharedstreets.tools.builder.transforms.Intersections;
import io.sharedstreets.tools.builder.osm.OSMDataStream;
import io.sharedstreets.tools.builder.transforms.BaseSegments;
import io.sharedstreets.tools.builder.transforms.SharedStreets;
import org.apache.commons.cli.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


public class ProcessPBF {

    static Logger LOG = LoggerFactory.getLogger(ProcessPBF.class);

    public static boolean DEBUG_OUTPUT = true;

    public static void main(String[] args) throws Exception {

        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();

        options.addOption( OptionBuilder.withLongOpt( "input" )
                .withDescription( "path to input OSM PBF file" )
                .hasArg()
                .withArgName("INPUT-FILE")
                .create() );

        options.addOption( OptionBuilder.withLongOpt( "output" )
                .withDescription( "path to output directory (will be created)" )
                .hasArg()
                .withArgName("OUTPUT-DIR")
                .create() );


        String inputFile = "";

        String outputPath = "";

        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );

            if( line.hasOption( "input" ) ) {
                // print the value of block-size
                inputFile = line.getOptionValue( "input" );
            }

            if( line.hasOption( "output" ) ) {
                // print the value of block-size
                outputPath = line.getOptionValue( "output" );
            }
        }
        catch( Exception exp ) {
            System.out.println( "Unexpected exception:" + exp.getMessage() );
            return;
        }

        File file = new File(inputFile);
        if(!file.exists()) {
            System.out.println( "Input file not found: "  + inputFile);
            return;
        }

        if(!file.getName().endsWith(".pbf")) {
            System.out.println( "Input file must end with .pbf: "  + inputFile);
            return;
        }

        File directory = new File(outputPath);

        if(directory.exists()) {
            System.out.println( "Output directory already exists: "  + outputPath);
            return;
        }

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // load osm data from PBF input
        OSMDataStream dataStream = new OSMDataStream(inputFile, env);

        // create OSM intersections
        Intersections intersections = new Intersections(dataStream);

        // build internal model for street network
        BaseSegments segments = new BaseSegments(dataStream, intersections);

        // build sharedstreets references, geometries, intersections and metadata
        SharedStreets streets = new SharedStreets(segments);


        // output sharedstreets data to json tiles
        streets.geometries.output(new SharedStreetsGeometryGeoJSONTileFormat<SharedStreetsGeometry>(outputPath)).setParallelism(1);

        streets.intersections.output(new SharedStreetsIntersectionGeoJSONTileFormat<SharedStreetsIntersection>(outputPath)).setParallelism(1);


        JsoniterSpi.registerTypeEncoder(SharedStreetsReference.class, new SharedStreetsReferenceJSONEncoder());
        streets.references.output(new JSONTileFormat<SharedStreetsReference>(outputPath, "reference")).setParallelism(1);


        JsoniterSpi.registerTypeEncoder(SharedStreetsOSMMetadata.class, new SharedStreetsOSMMetadataJSONEncoder());
        DataSet<SharedStreetsOSMMetadata> metadata = streets.geometries.map(new MapFunction<SharedStreetsGeometry, SharedStreetsOSMMetadata>() {
            @Override
            public SharedStreetsOSMMetadata map(SharedStreetsGeometry value) throws Exception {
                return value.metadata;
            }
        });

        metadata.output(new JSONTileFormat<SharedStreetsOSMMetadata>(outputPath, "metadata")).setParallelism(1);

        //LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss.SSS")
        env.execute();

    }

}


