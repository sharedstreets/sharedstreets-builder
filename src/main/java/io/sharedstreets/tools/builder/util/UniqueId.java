package io.sharedstreets.tools.builder.util;


import org.apache.commons.lang.builder.HashCodeBuilder;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

public class UniqueId implements Comparable {

    // TODO implement Flink Normlaized Key interfaces

    private static int BYTE_SIZE = 16;

    private byte[] bytes;
    private int hashCode;

    public UniqueId() {
        bytes = new byte[BYTE_SIZE];
        hashCode = 0;
    }

    public byte[] getBytes() {
        return this.bytes;
    }

    public void setBytes(byte[] b) throws Exception {
        if(b.length == BYTE_SIZE)
            bytes = b;
        else
            throw new Exception("invalid bytes lenth: expected "+  BYTE_SIZE + " got " + b.length);
    }


    public static UniqueId generate() {
        UniqueId uniqueId = new UniqueId();
        UUID uuid = UUID.randomUUID();

        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());

        uniqueId.bytes = bb.array();

        return uniqueId;
    }
    public static UniqueId fromString(String data) {
        UniqueId uniqueId = new UniqueId();

        ByteBuffer bb = ByteBuffer.wrap(Base58.decode(data));
        uniqueId.bytes = bb.array();

        return uniqueId;
    }

    @Override
    public String toString() {
        return Base58.encode(bytes);
    }

    @Override
    public int hashCode() {
        if(hashCode == 0)
            hashCode = new HashCodeBuilder(17, 31).append(bytes).hashCode();
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof UniqueId))
            return false;
        if (obj == this)
            return true;

        return Arrays.equals(this.bytes, ((UniqueId)obj).bytes);
    }

    @Override
    public int compareTo(Object obj) {

        if (!(obj instanceof UniqueId))
            return -1;
        if (obj == this)
            return 0;

        byte[] bytesA = this.bytes;
        byte[] bytesB = ((UniqueId) obj).bytes;

        for(int i = 0; i < BYTE_SIZE; i++){
            if(bytesA[i] == bytesB[i])
                continue;
            else
                return bytesA[i] - bytesB[i];
        }

        return 0;
    }
}
