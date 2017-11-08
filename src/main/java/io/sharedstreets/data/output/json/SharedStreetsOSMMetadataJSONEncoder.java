package io.sharedstreets.data.output.json;

import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.Encoder;
import io.sharedstreets.data.SharedStreetsOSMMetadata;

import java.io.IOException;

public class SharedStreetsOSMMetadataJSONEncoder implements Encoder {

    @Override
    public void encode(Object obj, JsonStream stream) throws IOException {
        if (obj.getClass().equals(SharedStreetsOSMMetadata.class)) {
            SharedStreetsOSMMetadata ssOSMMetadata = (SharedStreetsOSMMetadata) obj;

            stream.writeObjectStart();

            stream.writeObjectField("geometryId");
            stream.writeVal(ssOSMMetadata.geometryId.toString());
            stream.writeMore();

            stream.writeObjectField("waySections");
            stream.writeArrayStart();

            for(int i = 0; i < ssOSMMetadata.waySections.length; i++) {

                stream.writeObjectStart();


                stream.writeObjectField("wayId");
                stream.writeVal(ssOSMMetadata.waySections[i].wayId);
                stream.writeMore();

                stream.writeObjectField("roadClass");
                stream.writeVal(ssOSMMetadata.waySections[i].roadClass.getValue());
                stream.writeMore();

                stream.writeObjectField("oneWay");
                stream.writeVal(ssOSMMetadata.waySections[i].oneWay);
                stream.writeMore();

                stream.writeObjectField("roundabout");
                stream.writeVal(ssOSMMetadata.waySections[i].roundabout);
                stream.writeMore();

                stream.writeObjectField("link");
                stream.writeVal(ssOSMMetadata.waySections[i].link);
                stream.writeMore();

                stream.writeObjectField("nodeIds");
                stream.writeArrayStart();

                for(int j = 0; j < ssOSMMetadata.waySections[i].nodeIds.length; j++) {

                    stream.writeVal(ssOSMMetadata.waySections[i].nodeIds[j]);
                    if( j + 1 < ssOSMMetadata.waySections[i].nodeIds.length)
                        stream.writeMore();

                }
                stream.writeArrayEnd();

                stream.writeObjectEnd();

                if(i + 1 < ssOSMMetadata.waySections.length)
                    stream.writeMore();


            }
            stream.writeArrayEnd();
            stream.writeObjectEnd();
        }
    }
}