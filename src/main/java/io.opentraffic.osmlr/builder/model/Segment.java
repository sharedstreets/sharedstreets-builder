package io.opentraffic.osmlr.builder.model;


import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import io.opentraffic.osmlr.osm.model.NodePosition;
import io.opentraffic.osmlr.osm.model.SpatialEntity;

public class Segment extends SpatialEntity {

    WaySegment[] waySegments;

    NodePosition[] nodes;

    public Segment(Way ) {

    }

    @Override
    public Geometry constructGeometry() {
        return GeometryEngine.geometryFromEsriShape(shapeGeometry, geomType);
    }
}
