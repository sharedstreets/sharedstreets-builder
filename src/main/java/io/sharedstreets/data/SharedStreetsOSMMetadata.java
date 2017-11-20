package io.sharedstreets.data;

import com.google.protobuf.ByteString;
import com.jsoniter.annotation.JsonIgnore;
import io.sharedstreets.data.output.proto.SharedStreetsProto;
import io.sharedstreets.tools.builder.osm.model.Way;
import io.sharedstreets.tools.builder.model.BaseSegment;
import io.sharedstreets.tools.builder.model.WaySection;
import io.sharedstreets.tools.builder.tiles.TilableData;
import io.sharedstreets.tools.builder.util.UniqueId;
import io.sharedstreets.tools.builder.util.geo.TileId;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

public class SharedStreetsOSMMetadata implements Serializable {

    public class WaySectionMetadata implements Serializable {

        public Long wayId;
        public Way.ROAD_CLASS roadClass;
        public Boolean oneWay;
        public Boolean roundabout;
        public Boolean link;
        public Long[] nodeIds;

        public WaySectionMetadata( WaySection section) {
            this.wayId = section.wayId;

            this.roadClass = section.roadClass;
            this.oneWay = section.oneWay;
            this.roundabout = section.roundabout;
            this.link = section.link;

            this.nodeIds = new Long[section.nodes.length];

            for(int i = 0; i < section.nodes.length; i++) {
                this.nodeIds[i] = section.nodes[i].nodeId;
            }
        }
    }

    public UniqueId geometryId;
    public WaySectionMetadata[] waySections;


    public String getType() {
        return "metadata";
    }

    public byte[] toBinary() throws IOException {

        SharedStreetsProto.SharedStreetsMetadata.Builder metadata = SharedStreetsProto.SharedStreetsMetadata.newBuilder();

        metadata.setGeometryID(this.geometryId.toString());

        SharedStreetsProto.OSMMetadata.Builder osmMetadata = SharedStreetsProto.OSMMetadata.newBuilder();

        for(SharedStreetsOSMMetadata.WaySectionMetadata waySectionMetadata : this.waySections) {
            SharedStreetsProto.WaySection.Builder waySection = SharedStreetsProto.WaySection.newBuilder();

            waySection.setWayId(waySectionMetadata.wayId);
            waySection.setRoadClass(SharedStreetsProto.RoadClass.forNumber(waySectionMetadata.roadClass.getValue()));
            waySection.setOneWay(waySectionMetadata.oneWay);
            waySection.setLink(waySectionMetadata.link);
            waySection.setRoundabout(waySectionMetadata.roundabout);

            for(long nodeId : waySectionMetadata.nodeIds) {
                waySection.addNodeIds(nodeId);
            }

            osmMetadata.addWaySections(waySection);
        }

        metadata.setOsmMetadata(osmMetadata);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        metadata.build().writeDelimitedTo(bytes);

        return bytes.toByteArray();
    }

    public SharedStreetsOSMMetadata(SharedStreetsGeometry geometry, BaseSegment segment) {

        this.geometryId = geometry.id; // keeping reference for point data

        waySections = new WaySectionMetadata[segment.waySections.length];

        int i = 0;
        for(WaySection section : segment.waySections) {
            waySections[i] = new WaySectionMetadata(section);
            i++;
        }
    }

    public Long getStartNodeId() {
        return waySections[0].nodeIds[0];
    }

    public Long getEndNodeId() {
        return waySections[waySections.length - 1].nodeIds[waySections[waySections.length - 1].nodeIds.length-1];
    }

    public Way.ROAD_CLASS getRoadClass() {

        Way.ROAD_CLASS roadClass = null;

        for(WaySectionMetadata waySection : this.waySections) {
            if(roadClass == null || roadClass == waySection.roadClass)
                roadClass = waySection.roadClass;
            else {
                roadClass = Way.ROAD_CLASS.ClassOther;
                break;
            }
        }
        return roadClass;
    }
}
