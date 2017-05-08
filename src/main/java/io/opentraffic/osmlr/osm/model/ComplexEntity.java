package io.opentraffic.osmlr.osm.model;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;

public class ComplexEntity extends SpatialEntity {

    public Geometry.Type geomType;
    public byte[] shapeGeometry;

    @Override
    public Geometry constructGeometry() {
        return GeometryEngine.geometryFromEsriShape(shapeGeometry, geomType);
    }

}