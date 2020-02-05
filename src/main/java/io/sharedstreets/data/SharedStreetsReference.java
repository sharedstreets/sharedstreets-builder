package io.sharedstreets.data;


import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polyline;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int32Value;
import com.jsoniter.annotation.JsonIgnore;
import io.sharedstreets.data.output.proto.SharedStreetsProto;
import io.sharedstreets.tools.builder.osm.model.Way;
import io.sharedstreets.tools.builder.model.BaseSegment;
import io.sharedstreets.tools.builder.model.WaySection;
import io.sharedstreets.tools.builder.tiles.TilableData;
import io.sharedstreets.tools.builder.util.UniqueId;
import io.sharedstreets.tools.builder.util.geo.Geography;
import io.sharedstreets.tools.builder.util.geo.TileId;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SharedStreetsReference extends TilableData implements Serializable {

    public static double MAX_LPR_SEGMENT_LENGTH = 15000.0d; // meters
    public static double LPR_BEARING_OFFSET = 20.0d; // meters


    public enum FORM_OF_WAY {

        Undefined(0),
        Motorway(1),
        MultipleCarriageway(2),
        SingleCarriageway(3),
        Roundabout(4),
        TrafficSquare(5), // Like a roundabout but square? https://giphy.com/gifs/square-addis-meskel-GYb9s3Afw0cWA
        SlipRoad(6),
        Other(7);

        private final int value;

        FORM_OF_WAY(final int newValue) {
            value = newValue;
        }

        public int getValue() {
            return value;
        }
    };

    public UniqueId id;
    public FORM_OF_WAY formOfWay;
    public SharedStreetsLocationReference[] locationReferences;

    public SharedStreetsGeometry geometry;

    private final static Geography GeoOp = new Geography();

    public String getType() {
        return "reference";
    }

    public byte[] toBinary() throws IOException {

        SharedStreetsProto.SharedStreetsReference.Builder reference = SharedStreetsProto.SharedStreetsReference.newBuilder();

        reference.setId(this.getId());

        reference.setFormOfWay(SharedStreetsProto.SharedStreetsReference.FormOfWay.forNumber(this.formOfWay.getValue()));
        reference.setGeometryId(this.geometry.id.toString());

        for(SharedStreetsLocationReference locationReference : this.locationReferences) {

            SharedStreetsProto.LocationReference.Builder lr = SharedStreetsProto.LocationReference.newBuilder();

            lr.setIntersectionId(locationReference.intersection.id.toString());

            // optional values in proto3 require google Int32Value wrapper classes -- ugh!

            if(locationReference.distanceToNextRef != null)
                lr.setDistanceToNextRef((int)Math.round(locationReference.distanceToNextRef * 100)); // store in centimeter precision

            if(locationReference.inboundBearing != null)
                lr.setInboundBearing((int)Math.round(locationReference.inboundBearing));

            if(locationReference.outboundBearing != null)
                lr.setOutboundBearing((int)Math.round(locationReference.outboundBearing));


            lr.setLon(locationReference.point.getX());
            lr.setLat(locationReference.point.getY());

            reference.addLocationReferences(lr);
        }

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        reference.build().writeDelimitedTo(bytes);

        return bytes.toByteArray();

    }

    @Override
    @JsonIgnore
    public String getId() {

        if(id == null)
            this.id = generateId(this);

        return this.id.toString();
    }

    @Override
    @JsonIgnore
    public Set<TileId> getTileKeys(int zLevel) {
        HashSet<TileId> tileIdSet = new HashSet<>();

        for(int i = 0; i < locationReferences.length; i++) {

            tileIdSet.add(TileId.lonLatToTileId(zLevel, locationReferences[i].point.getX(), locationReferences[i].point.getY()));
        }

        return tileIdSet;
    }

    public static List<SharedStreetsReference> getSharedStreetsReferences(BaseSegment segment) {

        // generate single shared geometry for all references
        SharedStreetsGeometry geometry = new SharedStreetsGeometry(segment);

        List<SharedStreetsReference> list = new ArrayList<>();

        if(((Polyline)geometry.geometry).getPointCount() < 2)
            return list;

        FORM_OF_WAY formOfWay = SharedStreetsReference.getFormOfWay(segment);

        SharedStreetsReference reference1 = new SharedStreetsReference();

        reference1.formOfWay = formOfWay;

        List<SharedStreetsLocationReference> lprList = SharedStreetsReference.getLocationReferences(geometry, false);
        reference1.locationReferences = lprList.toArray(new SharedStreetsLocationReference[lprList.size()]);
        reference1.id = SharedStreetsReference.generateId(reference1);

        geometry.forwardReferenceId = reference1.id;
        geometry.startIntersectionId = reference1.locationReferences[0].intersection.id;
        geometry.endIntersectionId = reference1.locationReferences[reference1.locationReferences.length-1].intersection.id;

        reference1.geometry = geometry;
        list.add(reference1);

        // if not one-way generate reverse segments
        if(!segment.oneWay) {

            SharedStreetsReference reference2 = new SharedStreetsReference();
            reference2.formOfWay = formOfWay;

            lprList = SharedStreetsReference.getLocationReferences(geometry, true);
            reference2.locationReferences = lprList.toArray(new SharedStreetsLocationReference[lprList.size()]);

            reference2.id = SharedStreetsReference.generateId(reference2);

            geometry.backReferenceId = reference2.id;

            reference2.geometry = geometry;

            list.add(reference2);
        }

        return list;
    }

    public static List<SharedStreetsLocationReference> getLocationReferences(SharedStreetsGeometry geometry, boolean reverse) {

        List<SharedStreetsLocationReference> referenceList = new ArrayList<>();

        Polyline path;
        if(reverse) {
            // make a copy so we can reverse
            path = (Polyline)geometry.geometry.copy();
            path.reverseAllPaths();
        }
        else
            path = (Polyline)geometry.geometry;

        double length = geometry.length;

        int lprCount;

        // segments longer than 15km get intermediary LPRs
        if(length > MAX_LPR_SEGMENT_LENGTH) {
            // add one new LPR for every 15km in length, but split evenly over path
            // at 16km path has LPRs at 0,8,16 -- 35km path has 0,11.6,22.3,35
            lprCount =  (int)Math.ceil(length / MAX_LPR_SEGMENT_LENGTH);
        }
        else
            lprCount = 2;

        double lprSegmentLength = length / (lprCount - 1);

        // build LPRs
        for(int i = 0; i < lprCount; i++){

            SharedStreetsLocationReference lpr = new SharedStreetsLocationReference();

            lpr.sequence = i + 1;

            double fraction = 0.0d;

            if(i > 0.0d)
                fraction = (lprCount - 1) / i;

            lpr.point = GeoOp.interpolate(path, fraction);

            // final lpr doesn't have a distance to next point or outbound bearing
            if(i  < lprCount - 1) {
                lpr.distanceToNextRef = lprSegmentLength;

                Point outboundBearingPoint;

                // for segments shorter than LPR_BEARING_OFFSET just the bearing of the entire segment
                if (length > LPR_BEARING_OFFSET) {
                    // get point 20m further along line
                    double bearingPointOffset = LPR_BEARING_OFFSET / length;
                    outboundBearingPoint = GeoOp.interpolate(path, (fraction + bearingPointOffset));
                } else
                    outboundBearingPoint = GeoOp.interpolate(path, 1.0);

                // gets the bearing for the
                lpr.outboundBearing = GeoOp.azimuth(lpr.point, outboundBearingPoint, 1.0);
            }

            // initial lpr doesn't have an inbound bearing
            if(i > 0) {
                Point inboundBearingPoint;

                // for segments shorter than LPR_BEARING_OFFSET just the bearing of the entire segment
                if (length > LPR_BEARING_OFFSET) {
                    // get point 20m back along line
                    double bearingPointOffset = LPR_BEARING_OFFSET / length;
                    inboundBearingPoint = GeoOp.interpolate(path, (fraction - bearingPointOffset));
                } else
                    inboundBearingPoint = GeoOp.interpolate(path, 0.0);

                // gets the bearing for the
                lpr.inboundBearing = GeoOp.azimuth(inboundBearingPoint, lpr.point, 1.0);
            }


            SharedStreetsIntersection intersection = new SharedStreetsIntersection();
            intersection.point = lpr.point;

            // generate intersection node relationships for first LPR
            if(i == 0) {

                if(!reverse)
                    intersection.osmNodeId = geometry.metadata.getStartNodeId();
                else
                    intersection.osmNodeId = geometry.metadata.getEndNodeId();
            }
            else if(i == lprCount - 1) {
                // generate intersection node relationships for last LPR

                if(!reverse)
                    intersection.osmNodeId = geometry.metadata.getEndNodeId();
                else
                    intersection.osmNodeId = geometry.metadata.getStartNodeId();
            }
            else {
                // synthetic LPRs don't have node IDs
                intersection.osmNodeId = null;
            }

            intersection.id = SharedStreetsIntersection.generateId(intersection);

            lpr.intersection = intersection;

            referenceList.add(lpr);
        }

        return referenceList;
    }

    public static FORM_OF_WAY getFormOfWay(BaseSegment segment){

        // links roads (turing channel / ramps) are FoW slip roads
        if (segment.link) {
            return FORM_OF_WAY.SlipRoad;
        }
        else if(segment.roundabout) {
            return FORM_OF_WAY.Roundabout;
        }
        else {
            // find class for all way sections
            Way.ROAD_CLASS roadClass = null;
            for(WaySection section : segment.waySections) {
                if(roadClass == null)
                    roadClass = section.roadClass;
                else if(roadClass != section.roadClass) {
                    // if section isn't the same as previous section return FORM_OF_WAY.Undefined
                    return FORM_OF_WAY.Undefined;
                }
            }

            if(roadClass == Way.ROAD_CLASS.ClassMotorway)
                return FORM_OF_WAY.Motorway;
            else if((roadClass == Way.ROAD_CLASS.ClassPrimary || roadClass == Way.ROAD_CLASS.ClassTrunk)
                    && segment.oneWay) {
                // if primary or trunk road and one way assume (?) multiple carriageway
                return FORM_OF_WAY.MultipleCarriageway;
            }
            else if(roadClass == Way.ROAD_CLASS.ClassTrunk ||
                    roadClass == Way.ROAD_CLASS.ClassPrimary ||
                    roadClass == Way.ROAD_CLASS.ClassSecondary ||
                    roadClass == Way.ROAD_CLASS.ClassTertiary ||
                    roadClass == Way.ROAD_CLASS.ClassResidential ||
                    roadClass == Way.ROAD_CLASS.ClassUnclassified) {
                return FORM_OF_WAY.SingleCarriageway;
            }
            else
                return FORM_OF_WAY.Other;
        }
    }

    // generate a stable ref
    public static UniqueId generateId(SharedStreetsReference ssr) {
        String hashString = new String();

        hashString = "Reference " + ssr.formOfWay.value;

        for(SharedStreetsLocationReference lr : ssr.locationReferences) {
            hashString += String.format(" %.5f %.5f", lr.point.getX(), lr.point.getY());
            if(lr.outboundBearing != null) {
                hashString += String.format(" %d", Math.round(lr.outboundBearing));
                hashString += String.format(" %d", Math.round(lr.distanceToNextRef)); // hash of distance to next ref in meters -- stored in centimeters
            }
        }
        UniqueId id = UniqueId.generateHash(hashString);
        
        return id;
    }

}
