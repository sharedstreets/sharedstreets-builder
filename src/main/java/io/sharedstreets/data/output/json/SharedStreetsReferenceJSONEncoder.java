package io.sharedstreets.data.output.json;

import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.Encoder;
import io.sharedstreets.data.SharedStreetsReference;

import java.io.IOException;

public class SharedStreetsReferenceJSONEncoder implements Encoder {

    @Override
    public void encode(Object obj, JsonStream stream) throws IOException {
        if (obj.getClass().equals(SharedStreetsReference.class)) {
            SharedStreetsReference ssr = (SharedStreetsReference) obj;

            stream.writeObjectStart();

            stream.writeObjectField("geometryId");
            stream.writeVal(ssr.geometry.id.toString());
            stream.writeMore();

            stream.writeObjectField("formOfWay");
            stream.writeVal(ssr.formOfWay.getValue());
            stream.writeMore();

            stream.writeObjectField("locationReferences");

            stream.writeArrayStart();


            for(int i = 0; i < ssr.locationReferences.length; i++) {
                stream.writeObjectStart();

                stream.writeObjectField("point");
                    stream.writeArrayStart();
                        stream.writeVal(ssr.locationReferences[i].point.getX());
                        stream.writeMore();
                        stream.writeVal(ssr.locationReferences[i].point.getY());
                    stream.writeArrayEnd();

                if(ssr.locationReferences[i].outboundBearing != null) {

                    stream.writeMore();
                    stream.writeObjectField("outboundBearing");
                    stream.writeVal(Math.round(ssr.locationReferences[i].outboundBearing));
                    stream.writeMore();

                    stream.writeObjectField("distanceToNextRef");
                    stream.writeVal(Math.round(ssr.locationReferences[i].distanceToNextRef*100.0)/100.0d); // ship centimeter precision

                }

                if(ssr.locationReferences[i].inboundBearing != null) {

                    stream.writeMore();
                    stream.writeObjectField("inboundBearing");
                    stream.writeVal(Math.round(ssr.locationReferences[i].inboundBearing));

                }

                if(ssr.locationReferences[i].intersection != null) {
                    stream.writeMore();
                    stream.writeObjectField("intersectionId");
                    stream.writeVal(ssr.locationReferences[i].intersection.id.toString());

                }

                stream.writeObjectEnd();

                if(i + 1 < ssr.locationReferences.length)
                    stream.writeMore();
            }

            stream.writeArrayEnd();
            stream.writeObjectEnd();

        }
    }
}

