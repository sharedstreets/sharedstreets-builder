package io.sharedstreets.tools.builder.tiles;

/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/


import java.io.*;
import java.util.*;

import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.JsoniterSpi;
import io.sharedstreets.data.SharedStreetsGeometry;
import io.sharedstreets.data.SharedStreetsIntersection;
import io.sharedstreets.data.SharedStreetsOSMMetadata;
import io.sharedstreets.data.SharedStreetsReference;
import io.sharedstreets.data.output.json.SharedStreetsGeometryJSONEncoder;
import io.sharedstreets.data.output.json.SharedStreetsIntersectionJSONEncoder;
import io.sharedstreets.data.output.json.SharedStreetsOSMMetadataJSONEncoder;
import io.sharedstreets.data.output.json.SharedStreetsReferenceJSONEncoder;
import io.sharedstreets.tools.builder.util.geo.TileId;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.io.CleanupWhenUnsuccessful;
import org.apache.flink.api.common.io.InitializeOnMaster;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.FileSystem.WriteMode;

/**
 * The abstract base class for all Rich output formats that are file based. Contains the logic to
 * open/close the target
 * file streams.
 */
@Public
public class JSONTileOutputFormat<IT> extends FileOutputFormat<IT> implements InitializeOnMaster, CleanupWhenUnsuccessful {

    private static final long serialVersionUID = 1L;

    // --------------------------------------------------------------------------------------------

    /**
     * Behavior for creating output directories.
     */
    public static enum OutputDirectoryMode {

        /** A directory is always created, regardless of number of write tasks. */
        ALWAYS,

        /** A directory is only created for parallel output tasks, i.e., number of output tasks &gt; 1.
         * If number of output tasks = 1, the output is written to a single file. */
        PARONLY
    }

    // --------------------------------------------------------------------------------------------

    private static WriteMode DEFAULT_WRITE_MODE;

    private static String RECORD_DELIMITER = ",\n";

    private static OutputDirectoryMode DEFAULT_OUTPUT_DIRECTORY_MODE;

    static {
        initDefaultsFromConfiguration(GlobalConfiguration.loadConfiguration());
    }

    /**
     * Initialize defaults for output format. Needs to be a static method because it is configured for local
     * cluster execution, see LocalFlinkMiniCluster.
     * @param configuration The configuration to load defaults from
     */
    private static void initDefaultsFromConfiguration(Configuration configuration) {
        final boolean overwrite = configuration.getBoolean(
                ConfigConstants.FILESYSTEM_DEFAULT_OVERWRITE_KEY,
                ConfigConstants.DEFAULT_FILESYSTEM_OVERWRITE);

        DEFAULT_WRITE_MODE = overwrite ? WriteMode.OVERWRITE : WriteMode.NO_OVERWRITE;

        final boolean alwaysCreateDirectory = configuration.getBoolean(
                ConfigConstants.FILESYSTEM_OUTPUT_ALWAYS_CREATE_DIRECTORY_KEY,
                ConfigConstants.DEFAULT_FILESYSTEM_ALWAYS_CREATE_DIRECTORY);

        DEFAULT_OUTPUT_DIRECTORY_MODE = alwaysCreateDirectory ? OutputDirectoryMode.ALWAYS : OutputDirectoryMode.PARONLY;
    }

    // --------------------------------------------------------------------------------------------

    /**
     * The LOG for logging messages in this class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(JSONTileOutputFormat.class);

    /**
     * The key under which the name of the target path is stored in the configuration.
     */
    public static final String FILE_PARAMETER_KEY = "flink.output.file";

    /**
     * The path of the file to be written.
     */
    protected Path outputFilePath;

    /**
     * The write mode of the output.
     */
    private WriteMode writeMode;

    /**
     * The output directory mode
     */
    private OutputDirectoryMode outputDirectoryMode;

    // --------------------------------------------------------------------------------------------

    /** The stream to which the data is written; */
    private transient HashMap<String, FSDataOutputStream> streams;

    /** The path that is actually written to (may a a file in a the directory defined by {@code outputFilePath} ) */
    private transient Path actualFilePath;

    /** Flag indicating whether this format actually created a file, which should be removed on cleanup. */
    private transient boolean fileCreated;

    // --------------------------------------------------------------------------------------------


    private int openCount = 0;

    private boolean verbose;
    private  boolean metadata;

    HashSet<String> recordTypes = new HashSet<String>();

    public JSONTileOutputFormat(String path, boolean verbose, boolean metadata) {
        this.writeMode = FileSystem.WriteMode.OVERWRITE;
        this.outputFilePath = new Path(path);
        this.metadata = metadata;
        this.verbose = verbose;
    }

    public Path getOutputFilePath() {
        return this.outputFilePath;
    }


    public WriteMode getWriteMode() {
        return this.writeMode;
    }


    public void setOutputDirectoryMode(OutputDirectoryMode mode) {
        if (mode == null) {
            throw new NullPointerException();
        }

        this.outputDirectoryMode = mode;
    }


    // ----------------------------------------------------------------

    @Override
    public void configure(Configuration parameters) {
        // get the output file path, if it was not yet set
        if (this.outputFilePath == null) {
            // get the file parameter
            String filePath = parameters.getString(FILE_PARAMETER_KEY, null);
            if (filePath == null) {
                throw new IllegalArgumentException("The output path has been specified neither via constructor/setters" +
                        ", nor via the Configuration.");
            }

            try {
                this.outputFilePath = new Path(filePath);
            }
            catch (RuntimeException rex) {
                throw new RuntimeException("Could not create a valid URI from the given file path name: " + rex.getMessage());
            }
        }

        // check if have not been set and use the defaults in that case
        if (this.writeMode == null) {
            this.writeMode = DEFAULT_WRITE_MODE;
        }

        if (this.outputDirectoryMode == null) {
            this.outputDirectoryMode = DEFAULT_OUTPUT_DIRECTORY_MODE;
        }
    }


    @Override
    public void open(int taskNumber, int numTasks) throws IOException {

        if(this.streams == null)
            this.streams = new HashMap<>();

        Path p = this.outputFilePath;
        if (p == null) {
            throw new IOException("The file path is null.");
        }

        final FileSystem fs = p.getFileSystem();
        // if this is a local file system, we need to initialize the local output directory here
        if (!fs.isDistributedFS()) {

            if (numTasks == 1 && outputDirectoryMode == OutputDirectoryMode.PARONLY) {
                // output should go to a single file

                // prepare local output path. checks for write mode and removes existing files in case of OVERWRITE mode
                if(!fs.initOutPathLocalFS(p, writeMode, false)) {
                    // output preparation failed! Cancel task.
                    throw new IOException("Output path '" + p.toString() + "' could not be initialized. Canceling task...");
                }
            }
            else {
                // numTasks > 1 || outDirMode == OutputDirectoryMode.ALWAYS

                if(!fs.initOutPathLocalFS(p, writeMode, true)) {
                    // output preparation failed! Cancel task.
                    throw new IOException("Output directory '" + p.toString() + "' could not be created. Canceling task...");
                }
            }
        }

        // Suffix the path with the parallel instance index, if needed
        this.actualFilePath = p;

        // register json type encoders in open so parallel threads can
        JsoniterSpi.registerTypeEncoder(SharedStreetsGeometry.class, new SharedStreetsGeometryJSONEncoder(false, false));
        JsoniterSpi.registerTypeEncoder(SharedStreetsIntersection.class, new SharedStreetsIntersectionJSONEncoder());
        JsoniterSpi.registerTypeEncoder(SharedStreetsReference.class, new SharedStreetsReferenceJSONEncoder());
        JsoniterSpi.registerTypeEncoder(SharedStreetsOSMMetadata.class, new SharedStreetsOSMMetadataJSONEncoder());


        // at this point, the file creation must have succeeded, or an exception has been thrown
        this.openCount++;
    }



    private FSDataOutputStream getStream(String key) throws IOException {

        if(!streams.containsKey(key)) {

            final FileSystem fs = this.outputFilePath.getFileSystem();
            streams.put(key, fs.create(this.actualFilePath.suffix("/" + key), writeMode));
        }

        return streams.get(key);
    }

    @Override
    public void close() throws IOException {

        openCount--;

        HashSet<String> tileIds = new HashSet<String>();

        // close temporary output streams
        for (String key : streams.keySet()) {
            streams.get(key).flush();
            streams.get(key).close();

            tileIds.add(key.split("\\.")[0]);
        }


        // merge streams...
        if(openCount == 0 && recordTypes.size() == 3) {

            final FileSystem fs = this.outputFilePath.getFileSystem();


            for (String id : tileIds) {

                FSDataOutputStream mergedOutput = fs.create(this.actualFilePath.suffix("/" + id + ".json"), writeMode);

                Vector<InputStream> streams = new Vector<InputStream>();

                InputStream geometriesObj = new ByteArrayInputStream("{\"geometries\": {".getBytes("UTF-8"));
                streams.add(geometriesObj);

                FSDataInputStream geometries = fs.open(this.actualFilePath.suffix("/" + id + ".SharedStreetsGeometry"));
                streams.add(geometries);

                // only encode intersections in verbose output -- can be reconstructed from referencesr
                FSDataInputStream intersections = fs.open(this.actualFilePath.suffix("/" + id + ".SharedStreetsIntersection"));
                if(verbose) {
                    InputStream inersectionsObj = new ByteArrayInputStream("},\n\"intersections\":{".getBytes("UTF-8"));
                    streams.add(inersectionsObj);
                    streams.add(intersections);
                }

                InputStream referencesObj = new ByteArrayInputStream("},\"references\":{".getBytes("UTF-8"));
                streams.add(referencesObj);

                FSDataInputStream references = fs.open(this.actualFilePath.suffix("/" + id + ".SharedStreetsReference"));
                streams.add(references);

                InputStream closeObj = new ByteArrayInputStream("}}".getBytes("UTF-8"));
                streams.add(closeObj);


                SequenceInputStream mergedStream = new SequenceInputStream(streams.elements());

                IOUtils.copy(mergedStream, mergedOutput);

                mergedOutput.flush();
                mergedOutput.close();

                references.close();
                intersections.close();
                geometries.close();

                // clean up temp files
                fs.delete(this.actualFilePath.suffix("/" + id + ".SharedStreetsReference"), false);
                fs.delete(this.actualFilePath.suffix("/" + id + ".SharedStreetsIntersection"), false);
                fs.delete(this.actualFilePath.suffix("/" + id + ".SharedStreetsGeometry"), false);



            }
        }

    }

    @Override
    public void writeRecord(IT record) throws IOException {

        if(record instanceof Tuple2 && ((Tuple2) record).f1 instanceof TilableData) {

            TileId id = (TileId)((Tuple2) record).f0;
            TilableData data = (TilableData)((Tuple2) record).f1;

            String recordType = data.getClass().getSimpleName().toString();
            recordTypes.add(recordType);

            FSDataOutputStream stream = getStream(id + "." + recordType);

            String recordString = "";

            // only write delimiter if not first record
            if(stream.getPos() > 0)
                 recordString += RECORD_DELIMITER;


            recordString += "\""+ data.getId() + "\":" +  JsonStream.serialize(data);

            stream.write(recordString.getBytes("UTF-8"));
        }

    }

    /**
     * Initialization of the distributed file system if it is used.
     *
     * @param parallelism The task parallelism.
     */
    @Override
    public void initializeGlobal(int parallelism) throws IOException {
        // no-op TODO handle distributed FS?
    }

    @Override
    public void tryCleanupOnError() {
        if (this.fileCreated) {
            this.fileCreated = false;

            try {
                close();
            } catch (IOException e) {
                LOG.error("Could not properly close FileOutputFormat.", e);
            }

            try {
                FileSystem.get(this.actualFilePath.toUri()).delete(actualFilePath, false);
            } catch (FileNotFoundException e) {
                // ignore, may not be visible yet or may be already removed
            } catch (Throwable t) {
                LOG.error("Could not remove the incomplete file " + actualFilePath + '.', t);
            }
        }
    }


}