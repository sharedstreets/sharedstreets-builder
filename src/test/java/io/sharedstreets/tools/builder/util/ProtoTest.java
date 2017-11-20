package io.sharedstreets.tools.builder.util;

import io.sharedstreets.data.output.proto.SharedStreetsProto;
import org.junit.Test;

import java.io.FileInputStream;

import static org.junit.Assert.assertEquals;

public class ProtoTest {

    @Test
    public void evaluate() throws Exception {


        FileInputStream fs = new FileInputStream("/Users/kpw/workspace/sharedstreets-explorer/public/data/test3/11-602-769.reference.pbf");

        while(fs.available() > 0) {
            SharedStreetsProto.SharedStreetsReference ref = SharedStreetsProto.SharedStreetsReference.parseDelimitedFrom(fs);
        }

    }

}