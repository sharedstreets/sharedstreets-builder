package io.sharedstreets.tools.builder.tiles;


import com.jsoniter.output.JsonStream;
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
public class JSONTileFormat<T extends TilableData> extends TileOutputFormat<T> implements InputTypeConfigurable {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(JSONTileFormat.class);

    public static final String DEFAULT_LINE_DELIMITER = ",";

    private transient HashMap<String, JSONArrayTileWriter> writers;

    public JSONTileFormat(String path, String key) {
        super(path, key);
    }


    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);
        writers = new HashMap<>();
    }

    @Override
    public void close() throws IOException {

        for(JSONArrayTileWriter wrt : writers.values()) {
            if (wrt != null) {
                wrt.close();
            }
        }

        super.close();
    }

    public JSONArrayTileWriter getWriter(String key) throws IOException {

        if(!writers.containsKey(key)) {
            JSONArrayTileWriter writer = new JSONArrayTileWriter();
            writer.open(this.getStream(key));
            writers.put(key, writer);
        }

        return writers.get(key);
    }


    public void writeRecord(T element) throws IOException {

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

    @Override
    public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
        // no-op
    }
}