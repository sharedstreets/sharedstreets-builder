package io.sharedstreets.data.outputs;


import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.JsoniterSpi;
import io.sharedstreets.data.SharedStreetsGeometry;
import io.sharedstreets.tools.builder.osm.model.SpatialEntity;
import io.sharedstreets.tools.builder.tiles.GeoJSONTileWriter;
import io.sharedstreets.tools.builder.tiles.TileOutputFormat;
import io.sharedstreets.tools.builder.util.geo.TileId;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;


@PublicEvolving
public class SharedStreetsGeometryGeoJSONTileFormat<T extends SharedStreetsGeometry> extends TileOutputFormat<T> implements InputTypeConfigurable {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(SharedStreetsGeometryGeoJSONTileFormat.class);

    public static final String DEFAULT_LINE_DELIMITER = ",";


    private transient HashMap<String, GeoJSONTileWriter> writers;

    public SharedStreetsGeometryGeoJSONTileFormat(String path) {
        super(path, "geometry");
        JsoniterSpi.registerTypeEncoder(SharedStreetsGeometry.class, new SharedStreetsGeometryJSONEncoder());
    }


    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);
        writers = new HashMap<>();
    }

    @Override
    public void close() throws IOException {

        for(GeoJSONTileWriter wrt : writers.values()) {
            if (wrt != null) {
                wrt.close();
            }
        }

        super.close();
    }

    public GeoJSONTileWriter getWriter(String key) throws IOException {

        if(!writers.containsKey(key)) {
            GeoJSONTileWriter writer = new GeoJSONTileWriter();
            writer.open(this.getStream(key));
            writers.put(key, writer);
        }

        return writers.get(key);
    }


    public void writeRecord(SharedStreetsGeometry element) throws IOException {

        TileId tileId = element.getTileKey();

        if(tileId == null) {
            tileId = new TileId();
            tileId.x = 0;
            tileId.y = 0;
        }



        this.getWriter(tileId.toString()).writeRecord(JsonStream.serialize(element));

    }

    // --------------------------------------------------------------------------------------------
    @Override
    public String toString() {
        return "SharedStreetsGeometryGeoJSONTileFormat (path: " + this.getOutputFilePath() + ")";
    }

    /**
     *
     * The purpose of this method is solely to check whether the data type to be processed
     * is in fact a tuple type.
     */
    @Override
    public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
        if (type.getTypeClass().isAssignableFrom(SpatialEntity.class)) {
            throw new InvalidProgramException("The " + SharedStreetsGeometryGeoJSONTileFormat.class.getSimpleName() +
                    " can only be used to write SptailEntity data sets.");
        }
    }
}