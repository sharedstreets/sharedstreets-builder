package io.opentraffic.osmlr.builder.outputs;


import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

import com.esri.core.geometry.OperatorExportToGeoJson;
import com.esri.core.geometry.OperatorImportFromGeoJson;
import com.esri.core.geometry.SpatialReference;
import io.opentraffic.osmlr.osm.model.SpatialEntity;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.core.fs.Path;

import io.opentraffic.osmlr.osm.model.ComplexEntity;


@PublicEvolving
public class GeoJSONOutputFormat<T extends SpatialEntity> extends FileOutputFormat<T> implements InputTypeConfigurable {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(GeoJSONOutputFormat.class);

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


    public void writeRecord(SpatialEntity element) throws IOException {

        String geoJsonGeom = OperatorExportToGeoJson.local().execute(element.constructGeometry());

        // add the record delimiter
        if(!this.firstRecord)
            this.wrt.write(DEFAULT_LINE_DELIMITER);

        this.wrt.write(" { \"type\": \"Feature\", \"properties\": {}, \"geometry\":");

        this.wrt.write(geoJsonGeom);

        this.wrt.write(" }\n");

        this.firstRecord = false;
    }

    // --------------------------------------------------------------------------------------------
    @Override
    public String toString() {
        return "CsvOutputFormat (path: " + this.getOutputFilePath() + ")";
    }

    /**
     *
     * The purpose of this method is solely to check whether the data type to be processed
     * is in fact a tuple type.
     */
    @Override
    public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
        if (type.getTypeClass().isAssignableFrom(SpatialEntity.class)) {
            throw new InvalidProgramException("The " + GeoJSONOutputFormat.class.getSimpleName() +
                    " can only be used to write SptailEntity data sets.");
        }
    }
}