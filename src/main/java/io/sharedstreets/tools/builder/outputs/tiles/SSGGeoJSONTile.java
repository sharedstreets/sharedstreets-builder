package io.sharedstreets.tools.builder.outputs.tiles;


import com.esri.core.geometry.OperatorExportToGeoJson;
import io.sharedstreets.data.SharedStreetGeometry;
import io.sharedstreets.data.osm.model.SpatialEntity;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;


@PublicEvolving
public class SSGGeoJSONTile<T extends SharedStreetGeometry> extends FileOutputFormat<T> implements InputTypeConfigurable {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(SSGGeoJSONTile.class);

    public static final String DEFAULT_LINE_DELIMITER = ",";

    private transient Writer wrt;

    private boolean firstRecord = true;

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);
        this.wrt = new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096), StandardCharsets.UTF_8);

        this.wrt.write("{\n  \"type\": \"FeatureCollection\",\n  \"features\": [");
    }

    @Override
    public void close() throws IOException {
        if (wrt != null) {
            this.wrt.write("]\n}");

            this.wrt.flush();
            this.wrt.close();
        }
        super.close();
    }


    public void writeRecord(SharedStreetGeometry element) throws IOException {

        String geoJsonGeom = OperatorExportToGeoJson.local().execute(element.geometry);

        // add the record delimiter
        if(!this.firstRecord)
            this.wrt.write(DEFAULT_LINE_DELIMITER);

        this.wrt.write(" { \"type\": \"Feature\", \"properties\": {\"id\": \"" + element.id + "\"}, \"geometry\":");

        this.wrt.write(geoJsonGeom);

        this.wrt.write(" }\n");

        this.firstRecord = false;
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