package io.opentraffic.osmlr.osm.tools;

import org.apache.commons.net.util.Base64;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.Polyline;

public class GeometryTools {

    public static String toAscii(byte[] bytes) throws Exception {
        StringBuilder sb = new StringBuilder();
        String encodeBase64String = Base64.encodeBase64String(bytes);
        // remove extra \n
        String[] lines = encodeBase64String.split("\\r\\n");
        for (String s : lines) {
            sb.append(s);
        }
        return sb.toString();
    }

    public static byte[] fromAscii(String ascii) throws Exception {
        return Base64.decodeBase64(ascii);
    }

    public static Polyline polylineFromAscii(String ascii) throws Exception {
        return (Polyline) GeometryEngine.geometryFromEsriShape(fromAscii(ascii), Geometry.Type.Polyline);
    }

}