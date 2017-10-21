package io.sharedstreets.data;

import com.jsoniter.annotation.JsonIgnore;
import io.sharedstreets.tools.builder.osm.model.Way;
import io.sharedstreets.tools.builder.model.BaseSegment;
import io.sharedstreets.tools.builder.model.WaySection;
import io.sharedstreets.tools.builder.tiles.TilableData;
import io.sharedstreets.tools.builder.util.UniqueId;
import io.sharedstreets.tools.builder.util.geo.TileId;

public class SharedStreetsOSMMetadata implements TilableData {

    public class WaySectionMetadata {

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


    public TileId titlId;


    @JsonIgnore
    @Override
    public TileId getTileKey() {
        if(titlId == null)
            return null;

        return titlId;
    }

    public SharedStreetsOSMMetadata(SharedStreetsGeometry geometry, BaseSegment segment) {

        this.geometryId = geometry.id; // keeping reference for point data
        this.titlId = geometry.getTileKey();

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
                roadClass = Way.ROAD_CLASS.ClassServiceOther;
                break;
            }
        }
        return roadClass;
    }
}
