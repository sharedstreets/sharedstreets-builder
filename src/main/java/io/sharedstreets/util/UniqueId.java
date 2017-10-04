package io.sharedstreets.util;


import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.UUID;

public class UniqueId implements Value, Comparable<UniqueId> {

    long hiBits;
    long loBits;

    public static UniqueId generate() {
        UniqueId uniqueId = new UniqueId();
        UUID uuid = UUID.randomUUID();

        uniqueId.hiBits = uuid.getMostSignificantBits();
        uniqueId.loBits = uuid.getLeastSignificantBits();

        return uniqueId;
    }
    public static UniqueId fromString(String data) {
        UniqueId uniqueId = new UniqueId();

        uniqueId.hiBits = new BigInteger(data.split("-")[0], 16).longValue();
        uniqueId.loBits = new BigInteger(data.split("-")[1], 16).longValue();

        return uniqueId;
    }

    @Override
    public String toString() {
        return String.format("%x", hiBits) + "-" + String.format("%x", loBits);
    }

    @Override
    public boolean equals(Object o){

        if (this == o)
            return true;
        // null check
        if (o == null)
            return false;
        // type check and cast
        if (getClass() != o.getClass())
            return false;
        UniqueId id = (UniqueId) o;
        // field comparison

        return (this.hiBits == id.hiBits) && (this.loBits == id.loBits);
    }

    @Override
    public int compareTo(UniqueId o) {
        if(this.hiBits == o.hiBits) {
            if(this.loBits == o.loBits)
                return 0;
            else if(this.loBits < o.loBits) {
                return 1;
            }
            else
                return -1;
        }
        else {
            if(this.hiBits < o.hiBits)
                return 1;
            else
                return -1;
        }
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        out.writeLong(this.hiBits);
        out.writeLong(this.loBits);
    }

    @Override
    public void read(DataInputView in) throws IOException {
        this.hiBits = in.readLong();
        this.loBits = in.readLong();
    }
}
