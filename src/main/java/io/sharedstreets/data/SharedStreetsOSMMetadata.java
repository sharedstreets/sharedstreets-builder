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
        public String name;

        public String[] tagNames;
        public String[] tagValues;

        public WaySectionMetadata( WaySection section, boolean storeWaySegmentNames) {

            this.wayId = section.wayId;

            this.roadClass = section.roadClass;
            this.oneWay = section.oneWay;
            this.roundabout = section.roundabout;
            this.link = section.link;

            if(storeWaySegmentNames)
                this.name = section.name;

            this.nodeIds = new Long[section.nodes.length];

            for(int i = 0; i < section.nodes.length; i++) {
                this.nodeIds[i] = section.nodes[i].nodeId;
            }

            this.tagNames = new String[section.fields.keySet().size()];
            this.tagValues = new String[section.fields.keySet().size()];

            int i = 0;
            for (String key : section.fields.keySet()) {
                this.tagNames[i] = key;
                this.tagValues[i] = section.fields.get(key);
                i++;

            }
        }
    }

    public UniqueId geometryId;
    public String name;
    public WaySectionMetadata[] waySections;


    public String getType() {
        return "metadata";
    }

    public byte[] toBinary() throws IOException {

        SharedStreetsProto.SharedStreetsMetadata.Builder metadata = SharedStreetsProto.SharedStreetsMetadata.newBuilder();

        metadata.setGeometryId(this.geometryId.toString());

        SharedStreetsProto.OSMMetadata.Builder osmMetadata = SharedStreetsProto.OSMMetadata.newBuilder();

        if(this.name != null)
            osmMetadata.setName(this.name);

        for(SharedStreetsOSMMetadata.WaySectionMetadata waySectionMetadata : this.waySections) {
            SharedStreetsProto.WaySection.Builder waySection = SharedStreetsProto.WaySection.newBuilder();

            waySection.setWayId(waySectionMetadata.wayId);
            waySection.setRoadClass(SharedStreetsProto.RoadClass.forNumber(waySectionMetadata.roadClass.getValue()));
            waySection.setOneWay(waySectionMetadata.oneWay);
            waySection.setLink(waySectionMetadata.link);
            waySection.setRoundabout(waySectionMetadata.roundabout);

            if(waySectionMetadata.name != null)
                waySection.setName(waySectionMetadata.name);

            for(long nodeId : waySectionMetadata.nodeIds) {
                waySection.addNodeIds(nodeId);
            }

            for(int i = 0 ; i < waySectionMetadata.tagNames.length; i++) {
                SharedStreetsProto.OsmTag.Builder tag = SharedStreetsProto.OsmTag.newBuilder();
                tag.setKey(waySectionMetadata.tagNames[i]);
                tag.setValue(waySectionMetadata.tagValues[i]);
                waySection.addTags(tag);
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

        // store WaySegement names only if more than one segment or segments with different names
        boolean storeWaySegmentNames = false;
        String lastSegmentName = null;
        for(WaySection waySection : segment.waySections) {

            if(lastSegmentName == null) {
                lastSegmentName = waySection.name;
            }
            else {
                if(!lastSegmentName.equals(waySection.name))
                    storeWaySegmentNames = true;
            }
        }

        if(!storeWaySegmentNames)
            this.name = lastSegmentName;


        int i = 0;
        for(WaySection section : segment.waySections) {
            waySections[i] = new WaySectionMetadata(section, storeWaySegmentNames);
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
