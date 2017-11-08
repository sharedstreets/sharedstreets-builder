package io.sharedstreets.tools.builder.tiles;


import org.apache.flink.core.fs.FSDataOutputStream;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

public class JSONArrayTileWriter {
    

    public Writer writer;
    public void open(FSDataOutputStream stream) throws IOException {
        writer = new OutputStreamWriter(new BufferedOutputStream(stream, 4096), StandardCharsets.UTF_8);
    }

    public void writeRecord(String record) throws IOException {
        // add the record delimiter

        this.writer.write(record);
    }

    public void close() throws IOException {
        this.writer.flush();
        this.writer.close();
    }

}
