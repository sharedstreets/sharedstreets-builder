package io.sharedstreets.tools.builder.util;

import com.esri.core.geometry.Point;
import io.sharedstreets.data.SharedStreetsIntersection;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;


public class SharedStreetsIdTest {

    @Test
    public void evaluate() throws Exception {

        // test ID generation for sharedstreets data types
        // key is lon,lat (x,y) ordering for hash strings and

        SharedStreetsIntersection sharedStreetsIntersection = new SharedStreetsIntersection();

        sharedStreetsIntersection.point = new Point();
        sharedStreetsIntersection.osmNodeId = 123l;
        sharedStreetsIntersection.inboundSegmentIds = new UniqueId[0];
        sharedStreetsIntersection.outboundSegmentIds = new UniqueId[0];

        sharedStreetsIntersection.point.setX(-74.0090917); // longitude -> float precision becomes -74.00909
        sharedStreetsIntersection.point.setY(40.7260025); // latitude

        // should generate message Intersection -74.009094 40.726002
        assertEquals(UniqueId.generateHash("Intersection -74.00909 40.72600 123").toString(), "2456bffd0ffca31eaaeb81f627415e29");

        assertEquals(sharedStreetsIntersection.getId(), "2456bffd0ffca31eaaeb81f627415e29");

        byte[] data = sharedStreetsIntersection.toBinary();

        SharedStreetsIntersection sharedStreetsIntersection2 = SharedStreetsIntersection.fromBinary(data);

        assertEquals(sharedStreetsIntersection.point.getX(), sharedStreetsIntersection2.point.getX(), 0.000001);


    }

}