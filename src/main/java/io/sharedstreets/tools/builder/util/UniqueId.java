package io.sharedstreets.tools.builder.util;


import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.UUID;

public class UniqueId implements Comparable {

    // TODO implement Flink normalized key interfaces

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



    public static UniqueId generateHash(String hashInput) {

        try {
            byte[] bytesOfMessage = hashInput.getBytes("UTF-8");
            MessageDigest md = MessageDigest.getInstance("MD5");

            UniqueId uniqueId = new UniqueId();
            uniqueId.bytes = md.digest(bytesOfMessage);

            return uniqueId;
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }



    public static UniqueId fromString(String data) throws Exception {
        UniqueId uniqueId = new UniqueId();
        uniqueId.bytes = Hex.decodeHex(data.toCharArray());
        return uniqueId;
    }

    @Override
    public String toString() {
        return Hex.encodeHexString(this.bytes);
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
