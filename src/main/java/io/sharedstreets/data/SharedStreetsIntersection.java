package io.sharedstreets.data;

import com.esri.core.geometry.Point;
import com.google.protobuf.ByteString;
import com.jsoniter.annotation.JsonIgnore;
import io.sharedstreets.data.output.proto.SharedStreetsProto;
import io.sharedstreets.tools.builder.tiles.TilableData;
import io.sharedstreets.tools.builder.util.UniqueId;
import io.sharedstreets.tools.builder.util.geo.TileId;

import java.io.*;
import java.util.ArrayList;
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

        if(id == null)
            this.id = generateId(this);

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

        hashString = String.format("Intersection %.5f %.5f", ssi.point.getX(), ssi.point.getY());

        // synthetic LPRs don't have node IDs...
        if(ssi.osmNodeId != null)
            hashString += " " + ssi.osmNodeId.toString();

        return UniqueId.generateHash(hashString);

    }

    public String getType() {
        return "intersection";
    }

    public byte[] toBinary() throws IOException {

        SharedStreetsProto.SharedStreetsIntersection.Builder intersection =  SharedStreetsProto.SharedStreetsIntersection.newBuilder();

        intersection.setId(this.getId());

        intersection.setNodeId(this.osmNodeId);

        intersection.setLon(this.point.getX());
        intersection.setLat(this.point.getY());

        for(UniqueId inboundId : this.inboundSegmentIds) {

            intersection.addInboundReferenceIds(inboundId.toString());
        }

        for(UniqueId outboundId : this.outboundSegmentIds) {

            intersection.addOutboundReferenceIds(outboundId.toString());
        }

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        intersection.build().writeDelimitedTo(bytes);

        return bytes.toByteArray();
    }

    public static SharedStreetsIntersection fromBinary(byte[] data) throws Exception {
        InputStream is = new ByteArrayInputStream(data);
        SharedStreetsProto.SharedStreetsIntersection input = SharedStreetsProto.SharedStreetsIntersection.parseDelimitedFrom(is);

        SharedStreetsIntersection obj = new SharedStreetsIntersection();

        obj.point = new Point();
        obj.osmNodeId = input.getNodeId();
        obj.point.setX(input.getLon());
        obj.point.setY(input.getLat());

        ArrayList<UniqueId> inboundIdList = new ArrayList<>();

        for(String id : input.getInboundReferenceIdsList()){
            inboundIdList.add(UniqueId.fromString(id));
        }
        obj.inboundSegmentIds = inboundIdList.toArray(new UniqueId[inboundIdList.size()]);

        ArrayList<UniqueId> outboundIdList= new ArrayList<>();

        for(String id : input.getOutboundReferenceIdsList()){
            outboundIdList.add(UniqueId.fromString(id));
        }
        obj.outboundSegmentIds = outboundIdList.toArray(new UniqueId[outboundIdList.size()]);


        return obj;
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
