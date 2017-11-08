package io.sharedstreets.data.output.json;

import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.Encoder;
import io.sharedstreets.data.SharedStreetsIntersection;

import java.io.IOException;

public class SharedStreetsIntersectionJSONEncoder implements Encoder {

    private boolean verbose = false;

    @Override
    public void encode(Object obj, JsonStream stream) throws IOException {
        if(obj.getClass().equals(SharedStreetsIntersection.class)){
            SharedStreetsIntersection ssi = (SharedStreetsIntersection)obj;

            stream.writeObjectStart();


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
                stream.writeMore();

            }

            stream.writeObjectField("point");
            stream.writeArrayStart();

            com.esri.core.geometry.Point point = ssi.point;

            stream.writeVal(point.getX());
            stream.writeMore();
            stream.writeVal(point.getY());

            stream.writeArrayEnd();
            stream.writeObjectEnd();

        }
    }
}