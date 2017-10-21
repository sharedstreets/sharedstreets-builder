package io.sharedstreets.data.outputs;

import com.esri.core.geometry.Point;
import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.Encoder;
import io.sharedstreets.data.SharedStreetsIntersection;

import java.io.IOException;

public class SharedStreetsIntersectionJSONEncoder implements Encoder {

    @Override
    public void encode(Object obj, JsonStream stream) throws IOException {
        if(obj.getClass().equals(SharedStreetsIntersection.class)){
            SharedStreetsIntersection ssi = (SharedStreetsIntersection)obj;

            stream.writeObjectStart();

            stream.writeObjectField("type");
            stream.writeVal("Feature");
            stream.writeMore();

            stream.writeObjectField("properties");
            stream.writeObjectStart();

            stream.writeObjectField("id");
            stream.writeVal(ssi.id.toString());
            stream.writeMore();

            if(ssi.osmNodeId != null) {
                stream.writeObjectField("osmNodeId");
                stream.writeVal(ssi.osmNodeId);
                stream.writeMore();
            }

            if(ssi.outboundSegmentIds != null) {
                stream.writeObjectField("outboundSegmentIds");
                stream.writeArrayStart();
                for(int i = 0; i < ssi.outboundSegmentIds.length; i++) {
                    stream.writeVal(ssi.outboundSegmentIds[i].toString());
                    if(i < ssi.outboundSegmentIds.length - 1)
                        stream.writeMore();
                }
                stream.writeArrayEnd();
                stream.writeMore();
            }

            if(ssi.inboundSegmentIds != null) {
                stream.writeObjectField("inboundSegmentIds");
                stream.writeArrayStart();
                for(int i = 0; i < ssi.inboundSegmentIds.length; i++) {
                    stream.writeVal(ssi.inboundSegmentIds[i].toString());
                    if(i < ssi.inboundSegmentIds.length - 1)
                        stream.writeMore();
                }
                stream.writeArrayEnd();
            }

            stream.writeObjectEnd();
            stream.writeMore();

            stream.writeObjectField("geometry");
            stream.writeObjectStart();

            stream.writeObjectField("type");
            stream.writeVal("Point");
            stream.writeMore();

            stream.writeObjectField("coordinates");
            stream.writeArrayStart();

            com.esri.core.geometry.Point point = ((Point)ssi.geometry);

            stream.writeVal(point.getX());
            stream.writeMore();
            stream.writeVal(point.getY());

            stream.writeArrayEnd();
            stream.writeObjectEnd();
            stream.writeObjectEnd();

        }
    }
}