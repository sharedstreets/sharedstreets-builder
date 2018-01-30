package io.sharedstreets.tools.builder.util;

import com.esri.core.geometry.Point;
import io.sharedstreets.data.SharedStreetsIntersection;
import org.junit.Test;

import static org.junit.Assert.*;


public class UniqueIdTest {

    @Test
    public void evaluate() {

        // hash test from https://github.com/sharedstreets/sharedstreets-ref-system/issues/8
        assertEquals(UniqueId.generateHash("Intersection 110.000000 45.000000").toString(), "71f34691f182a467137b3d37265cb3b6");
        assertEquals(UniqueId.generateHash("Intersection -74.003388 40.634538").toString(), "103c2dbe16d28cdcdcd5e5e253eaa026");
        assertEquals(UniqueId.generateHash("Intersection -74.004107 40.634060").toString(), "0f346cb98b5d8f0500e167cb0a390266");
        assertEquals(UniqueId.generateHash("Intersection -74.009094 40.726002").toString(), "6f1c08066370129f2f429f3c20f9e151");


        // test decoding/encoding of hash string
        UniqueId uniqueId1 = UniqueId.generateHash("Intersection 110.000000 45.000000");

        String idStr = uniqueId1.toString();

        UniqueId uniqueId2 = null;
        try {
            uniqueId2 = UniqueId.fromString(idStr);
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertEquals(uniqueId1, uniqueId2);


        SharedStreetsIntersection sharedStreetsIntersection = new SharedStreetsIntersection();

        sharedStreetsIntersection.point = new Point();
        sharedStreetsIntersection.point.setX(-74.00909423828125);
        sharedStreetsIntersection.point.setY(40.72600173950195);

        assertEquals(sharedStreetsIntersection.getId(), "6f1c08066370129f2f429f3c20f9e151");


    }

}