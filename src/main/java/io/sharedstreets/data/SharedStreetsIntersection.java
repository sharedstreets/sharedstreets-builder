package io.sharedstreets.data;

import com.esri.core.geometry.Point;
import com.google.protobuf.ByteString;
import com.jsoniter.annotation.JsonIgnore;
import io.sharedstreets.data.output.proto.SharedStreetsProto;
import io.sharedstreets.tools.builder.tiles.TilableData;
import io.sharedstreets.tools.builder.util.UniqueId;
import io.sharedstreets.tools.builder.util.geo.TileId;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class SharedStreetsIntersection extends TilableData implements Comparable, Serializable {

    public UniqueId id;
    public Long osmNodeId;
    public Point point;
    public UniqueId[] inboundSegmentIds;
    public UniqueId[] outboundSegmentIds;

    // TODO this is where turn restrictions go...

    @Override
    @JsonIgnore
    public String getId() {
        return this.id.toString();
    }

    @JsonIgnore
    public Set<TileId> getTileKeys(int zLevel) {
        HashSet<TileId> tileIdSet = new HashSet<>();

        tileIdSet.add(TileId.lonLatToTileId(zLevel, point.getX(), point.getY()));

        return tileIdSet;
    }

    public static UniqueId generateId(SharedStreetsIntersection ssi) {
        String hashString = new String();

        hashString = String.format("Intersection %.6f %.6f", ssi.point.getX(), ssi.point.getY());

        return UniqueId.generateHash(hashString);

    }

    public String getType() {
        return "intersection";
    }

    public byte[] toBinary() throws IOException {

        SharedStreetsProto.SharedStreetsIntersection.Builder intersection =  SharedStreetsProto.SharedStreetsIntersection.newBuilder();

        intersection.setId(this.id.toString());

        intersection.setNodeId(this.osmNodeId);

        intersection.setLon((float)this.point.getX());
        intersection.setLat((float)this.point.getY());

        for(UniqueId inboundId : this.inboundSegmentIds) {

            intersection.addInboundReferenceIds(inboundId.toString());
        }

        for(UniqueId outboundId : this.outboundSegmentIds) {

            intersection.addInboundReferenceIds(outboundId.toString());
        }

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        intersection.build().writeDelimitedTo(bytes);

        return bytes.toByteArray();
    }

    @Override
    public int compareTo(Object o) {
        return this.id.compareTo(o);
    }

    @Override
    public int hashCode() {
        return this.id.hashCode();
    }
}
