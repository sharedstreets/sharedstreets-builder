package io.sharedstreets.tools.builder.osm.model;

import com.esri.core.geometry.Geometry;

public class Way extends SpatialEntity {

    public enum ROAD_CLASS {

        ClassMotorway(0),
        ClassTrunk(1),
        ClassPrimary(2),
        ClassSecondary(3),
        ClassTertiary(4),
        ClassResidential(5),
        ClassUnclassified(6),
        ClassService(7),
        ClassOther(8);

        private final int value;

        ROAD_CLASS(final int newValue) {
            value = newValue;
        }

        public int getValue() {
            return value;
        }

    }

    public NodePosition[] nodes;

    public boolean isHighway() {

        if (fields == null)
            return false;

        assert fields != null;

        return fields.containsKey("highway");

    }

    public boolean isLink() {

        if (fields.containsKey("highway") && fields.get("highway").toLowerCase().trim().endsWith("_link"))
            return true;

        return false;

    }

    public boolean isDrivable() {

        if(this.isHighway()) {
            if((fields.containsKey("motorcar'") && fields.get("motorcar").toLowerCase().equals("no") || fields.get("motorcar").toLowerCase().equals("false")) ||
               (fields.containsKey("motor_vehicle") && fields.get("motor_vehicle").toLowerCase().equals("no") || fields.get("motor_vehicle").toLowerCase().equals("false")) ||
               (fields.containsKey("vehicle") && fields.get("vehicle").toLowerCase().equals("no") || fields.get("vehicle").toLowerCase().equals("false")) ||
                            (fields.containsKey("access") && fields.get("motorcar").toLowerCase().equals("no") || fields.get("motorcar").toLowerCase().equals("false"))){
                return false;
            }

            return true;
        }
        else
            return false;

    }

    public boolean isBikeable() {

        if (fields == null)
            return false;

        assert fields != null;

        return fields.containsKey("bikable");

    }

    public boolean isWalkable() {

        if (fields == null)
            return false;

        assert fields != null;

        return fields.containsKey("highway");

    }

    public String getName() {

        String name = "";

        if(fields.containsKey("name"))
            name = fields.get("name").trim();

        if(fields.containsKey("ref")) {
            if(name.isEmpty())
                name = fields.get("ref").trim();
            else
                name += "; "  + fields.get("ref").trim();
        }

        return name;
    }

    public ROAD_CLASS roadClass() {


        if(fields.containsKey("highway")) {
            if (fields.containsKey("highway") && fields.get("highway").toLowerCase().trim().startsWith("motorway"))
                return ROAD_CLASS.ClassMotorway;
            else if (fields.containsKey("highway") && fields.get("highway").toLowerCase().trim().startsWith("trunk"))
                return ROAD_CLASS.ClassTrunk;
            else if (fields.containsKey("highway") && fields.get("highway").toLowerCase().trim().startsWith("primary"))
                return ROAD_CLASS.ClassPrimary;
            else if (fields.containsKey("highway") && fields.get("highway").toLowerCase().trim().startsWith("secondary"))
                return ROAD_CLASS.ClassSecondary;
            else if (fields.containsKey("highway") && fields.get("highway").toLowerCase().trim().startsWith("tertiary"))
                return ROAD_CLASS.ClassTertiary;
            else if (fields.containsKey("highway") && fields.get("highway").toLowerCase().trim().startsWith("unclassified"))
                return ROAD_CLASS.ClassUnclassified;
            else if (fields.containsKey("highway") && fields.get("highway").toLowerCase().trim().startsWith("residential"))
                return ROAD_CLASS.ClassResidential;
            else if (fields.containsKey("highway") && fields.get("highway").toLowerCase().trim().startsWith("service")) {

                // attempt to exclude parking lots, driveways and other private driveways to keep just public services roads
                // not consistently mapped in OSM... https://taginfo.openstreetmap.org/keys/?key=service#values
                if (fields.containsKey("service") && (fields.get("service").toLowerCase().trim().startsWith("parking") ||
                        fields.get("service").toLowerCase().trim().startsWith("driveway") ||
                        fields.get("service").toLowerCase().trim().startsWith("drive-through")))
                    return ROAD_CLASS.ClassOther;
                else
                    return ROAD_CLASS.ClassService;


            } else if (fields.containsKey("highway") && fields.get("highway").toLowerCase().trim().startsWith("living_street"))
                return ROAD_CLASS.ClassResidential;
            else
                return ROAD_CLASS.ClassOther;
        }
        else {
            return ROAD_CLASS.ClassOther; // fall back to "other" for unknown
        }
    }

    public boolean isRoundabout() {

        if (fields.containsKey("junction") && fields.get("junction").toLowerCase().trim().equals("roundabout"))
            return true;

        return false;
    }

    public boolean isOneWay() {

        if (fields == null)
            return false;

        assert fields != null;

        String oneway = new String();
        if(fields.containsKey("oneway"))
            oneway = fields.get("oneway").toLowerCase().trim();

        // follow explicit case
        if(oneway.equals("yes") || oneway.equals("1") || oneway.equals("true"))
            return true;
        if(oneway.equals("no") || oneway.equals("0") || oneway.equals("false"))
            return false;

        // if not explicitly set, check for implied oneways
        if(fields.containsKey("highway") && fields.get("highway").toLowerCase().trim().equals("motorway"))
            return true;
        if(fields.containsKey("junction") && fields.get("junction").toLowerCase().trim().equals("roundabout"))
            return true;

        // otherwise false
        return false;

    }

    @Override
    public Geometry constructGeometry() {
        return null; // GeometryEngine.geometryFromEsriShape(shapeGeometry, geomType);
    }

}