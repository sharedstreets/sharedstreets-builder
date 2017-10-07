package io.sharedstreets.tools.builder.outputs.tiles;


import com.esri.core.geometry.OperatorExportToGeoJson;
import io.sharedstreets.data.SharedStreetGeometry;
import io.sharedstreets.data.osm.model.SpatialEntity;
import io.sharedstreets.util.TileId;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;


@PublicEvolving
public class SSGGeoJSONTile<T extends SharedStreetGeometry> extends TileOutputFormat<T> implements InputTypeConfigurable {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(SSGGeoJSONTile.class);

    public static final String DEFAULT_LINE_DELIMITER = ",";

    private transient HashMap<String, GeoJSONTileWriter> writers;

    public SSGGeoJSONTile(String path) {
        super(path);
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


    public void writeRecord(SharedStreetGeometry element) throws IOException {

        String geoJsonGeom = OperatorExportToGeoJson.local().execute(element.geometry);

        TileId tileId = element.getTileKey();

        if(tileId == null) {
            tileId = new TileId();
            tileId.x = 0;
            tileId.y = 0;
        }

        this.getWriter(tileId.toString()).writeRecord(" { \"type\": \"Feature\", \"properties\": {\"id\": \"" + element.id + "\", \"wayIds\": \"" + element.wayIds + "\"}, \"geometry\":" +  geoJsonGeom + " }\n");

    }

    // --------------------------------------------------------------------------------------------
    @Override
    public String toString() {
        return "SSGGeoJSONTile (path: " + this.getOutputFilePath() + ")";
    }

    /**
     *
     * The purpose of this method is solely to check whether the data type to be processed
     * is in fact a tuple type.
     */
    @Override
    public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
        if (type.getTypeClass().isAssignableFrom(SpatialEntity.class)) {
            throw new InvalidProgramException("The " + SSGGeoJSONTile.class.getSimpleName() +
                    " can only be used to write SptailEntity data sets.");
        }
    }
}