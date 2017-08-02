package io.opentraffic.osmlr.osm.model;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;

public class Way extends SpatialEntity {

    NodePosition[] nodes;

    @Override
    public Geometry constructGeometry() {
        return GeometryEngine.geometryFromEsriShape(shapeGeometry, geomType);
    }

}