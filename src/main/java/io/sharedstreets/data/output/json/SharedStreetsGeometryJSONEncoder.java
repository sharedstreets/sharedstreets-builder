package io.sharedstreets.data.output.json;

import com.esri.core.geometry.Polyline;
import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.Encoder;
import io.sharedstreets.data.SharedStreetsGeometry;

import java.io.IOException;

public class SharedStreetsGeometryJSONEncoder implements Encoder {

    private boolean verbose = false;
    private boolean metadata = false;

    public SharedStreetsGeometryJSONEncoder(boolean verbose, boolean metadata) {
        this.verbose = verbose;
        this.metadata = metadata;
    }

    @Override
    public void encode(Object obj, JsonStream stream) throws IOException {
        if(obj.getClass().equals(SharedStreetsGeometry.class)){
            SharedStreetsGeometry ssg = (SharedStreetsGeometry)obj;

            stream.writeObjectStart();

            // verbose output contains reference and intersection ids (can be recreated from non-verbose output)
            if(verbose) {
                if (ssg.startIntersectionId != null) {
                    stream.writeObjectField("startIntersectionId");
                    stream.writeVal(ssg.startIntersectionId.toString());
                    stream.writeMore();
                }

                if (ssg.endIntersectionId != null) {
                    stream.writeObjectField("endIntersectionId");
                    stream.writeVal(ssg.endIntersectionId.toString());
                    stream.writeMore();
                }

                if (ssg.forwardReferenceId != null) {
                    stream.writeObjectField("forwardReferenceId");
                    stream.writeVal(ssg.forwardReferenceId.toString());
                    stream.writeMore();
                }

                if (ssg.backReferenceId != null) {
                    stream.writeObjectField("backReferenceId");
                    stream.writeVal(ssg.backReferenceId.toString());
                    stream.writeMore();
                }
            }


            if(metadata) {
                stream.writeObjectField("metadata");
                stream.writeVal(ssg.metadata);
                stream.writeMore();
            }
            else {
                stream.writeObjectField("roadClass");
                stream.writeVal(ssg.metadata.getRoadClass().getValue());
                stream.writeMore();
            }


            stream.writeObjectField("coordinates");
            stream.writeArrayStart();
            int pointCount = ((Polyline)ssg.geometry).getPointCount();
            for(int i = 0; i < pointCount; i++) {
                com.esri.core.geometry.Point point = ((Polyline)ssg.geometry).getPoint(i);
                stream.writeArrayStart();
                stream.writeVal(point.getX());
                stream.writeMore();
                stream.writeVal(point.getY());
                stream.writeArrayEnd();
                if(i + 1 < pointCount)
                    stream.writeMore();
            }

            stream.writeArrayEnd();
            stream.writeObjectEnd();

        }
    }
}