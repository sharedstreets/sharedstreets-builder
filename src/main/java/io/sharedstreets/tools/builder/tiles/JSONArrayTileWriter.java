package io.sharedstreets.tools.builder.tiles;


import org.apache.flink.core.fs.FSDataOutputStream;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

public class JSONArrayTileWriter {

    public static final String DEFAULT_LINE_DELIMITER = ",";

    public Writer writer;
    public boolean firstRecord =true;

    public void open(FSDataOutputStream stream) throws IOException {
        writer = new OutputStreamWriter(new BufferedOutputStream(stream, 4096), StandardCharsets.UTF_8);

        writer.write("[");
    }

    public void writeRecord(String record) throws IOException {
        // add the record delimiter
        if(!this.firstRecord)
            this.writer.write(DEFAULT_LINE_DELIMITER);

        this.writer.write(record);

        this.firstRecord = false;
    }

    public void close() throws IOException {
        this.writer.write("]");
        this.writer.flush();
        this.writer.close();
    }

}
