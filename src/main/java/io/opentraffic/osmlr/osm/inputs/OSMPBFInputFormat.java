package io.opentraffic.osmlr.osm.inputs;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import io.opentraffic.osmlr.osm.model.AttributedEntity;
import io.opentraffic.osmlr.osm.model.NodeEntity;
import io.opentraffic.osmlr.osm.parser.Parser;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import crosby.binary.Fileformat;
import crosby.binary.Fileformat.Blob;
import crosby.binary.Fileformat.BlobHeader;
import crosby.binary.Osmformat.PrimitiveBlock;

/**
 * OSMPBF Input Format
 *
 * @author pfreydiere
 *
 */
public abstract class OSMPBFInputFormat<T> extends FileInputFormat<T> {

    private static final String OSM_DATA_HEADER = "OSMData";

    @Override
    public FileInputSplit[] createInputSplits(int minNumSplits) throws IOException {

        if (minNumSplits < 1) {
            throw new IllegalArgumentException("Number of input splits has to be at least 1.");
        }

        // take the desired number of splits into account
        minNumSplits = Math.max(minNumSplits, this.numSplits);

        final Path path = this.filePath;

        final List<FileInputSplit> inputSplits = new ArrayList<FileInputSplit>(minNumSplits);

        // get all the files that are involved in the splits
        List<FileStatus> files = new ArrayList<FileStatus>();

        final FileSystem fs = path.getFileSystem();
        final FileStatus pathFile = fs.getFileStatus(path);

        files.add(pathFile);

        int splitNum = 0;
        for (final FileStatus file : files) {

            final BlockLocation[] blocks = fs.getFileBlockLocations(file, 0, file.getLen());
            Set<String> hosts = new HashSet<String>();
            for (BlockLocation block : blocks) {
                hosts.addAll(Arrays.asList(block.getHosts()));
            }

            FSDataInputStream in = fs.open(path);
            try {
                DataInputStream dis = new DataInputStream(in);
                long pos = 0;
                while (in.available() > 0) {
                    int len = dis.readInt();
                    byte[] blobHeader = new byte[len];
                    in.read(blobHeader);
                    BlobHeader h = BlobHeader.parseFrom(blobHeader);
                    FileInputSplit split = new FileInputSplit(splitNum++, file.getPath(), pos, len + h.getDatasize(),
                            hosts.toArray(new String[hosts.size()]));
                    inputSplits.add(split);
                    pos += 4;
                    pos += len;
                    pos += h.getDatasize();
                    in.skip(h.getDatasize());
                }
            } catch (IOException e) {
                System.out.println(e.getLocalizedMessage());
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (Exception e) {
                    }
                }
            }

        }
        return inputSplits.toArray(new FileInputSplit[inputSplits.size()]);

    }

    @Override
    public void open(FileInputSplit fileSplit) throws IOException {
        super.open(fileSplit);
        current = null;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return current == null && stream.getPos() >= splitStart + splitLength;
    }

    Parser<NodeEntity> current;

    // NodeParser current;

    protected abstract Parser createParser(PrimitiveBlock p);

    @Override
    public T nextRecord(T reuse) throws IOException {

        try {

            if (current == null) {

                if (reachedEnd())
                    return null;

                // System.out.println(currentSplit + "read chunk");
                PrimitiveBlock p = readChunk(new DataInputStream(stream));

                while (p == null && !reachedEnd()) {
                    // System.out.println(currentSplit + "non data block,
                    // continue");
                    p = readChunk(new DataInputStream(stream));
                }

                if (p == null)
                    return null;

                current = createParser(p);

            }

            assert current != null;
            AttributedEntity n = current.next();
            if (n == null) {
                // System.out.println(currentSplit + "end of the split ,parser
                // returned null");
                current = null;
                return nextRecord(reuse);
            }

            // System.out.println("return " + n);
            return (T) n;

        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            throw new IOException(ex.getMessage(), ex);
        }

    }

    /**
     * read data chunk
     *
     * @param datinput
     * @throws IOException
     * @throws InvalidProtocolBufferException
     */
    private PrimitiveBlock readChunk(DataInputStream datinput) throws Exception {

        assert datinput != null;

        int headersize = datinput.readInt();

        byte buf[] = new byte[headersize];
        datinput.readFully(buf);

        // System.out.format("Read buffer for header of %d bytes\n",buf.length);
        Fileformat.BlobHeader header = Fileformat.BlobHeader.parseFrom(buf);

        int datasize = header.getDatasize();

        // System.out.println("block type :" + header.getType());
        // System.out.println("datasize :" + datasize);

        byte b[] = new byte[datasize];
        datinput.readFully(b);

        Blob blob = Fileformat.Blob.parseFrom(b);

        if (OSM_DATA_HEADER.equals(header.getType())) {
            return parseData(blob);
        }

        return null;

    }

    /**
     * Parse out and decompress the data part of a fileblock helper function.
     */
    private PrimitiveBlock parseData(Fileformat.Blob blob) throws Exception {

        assert blob != null;

        if (blob.hasRaw()) {

            return parsePrimitiveBlock(blob.getRaw());

        } else if (blob.hasZlibData()) {
            byte buf2[] = new byte[blob.getRawSize()];
            Inflater decompresser = new Inflater();
            decompresser.setInput(blob.getZlibData().toByteArray());
            // decompresser.getRemaining();
            try {
                decompresser.inflate(buf2);
            } catch (DataFormatException e) {
                e.printStackTrace();
                throw new Exception(e.getMessage(), e);
            }
            assert(decompresser.finished());
            decompresser.end();

            return parsePrimitiveBlock(ByteString.copyFrom(buf2));

        }

        throw new Exception("unsupported blob");
    }

    private PrimitiveBlock parsePrimitiveBlock(ByteString datas) throws Exception {

        return PrimitiveBlock.parseFrom(datas);
    }

}