package io.sharedstreets.tools.builder.transforms;


import io.sharedstreets.data.SharedStreetGeometry;
import io.sharedstreets.data.SharedStreetReference;
import io.sharedstreets.data.osm.OSMDataStream;
import io.sharedstreets.tools.builder.model.BaseSegment;
import io.sharedstreets.util.UniqueId;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.List;

public class SharedStreets implements Serializable {

    public DataSet<SharedStreetReference> references;
    public DataSet<SharedStreetGeometry> geometries;

    public SharedStreets(BaseSegments segments) {

        DataSet<SharedStreetReference> allReferences = segments.segments.flatMap(new FlatMapFunction<BaseSegment, SharedStreetReference>() {
            @Override
            public void flatMap(BaseSegment value, Collector<SharedStreetReference> out) throws Exception {
                List<SharedStreetReference> references = SharedStreetReference.getSharedStreetsReferences(value);

                for(SharedStreetReference reference : references) {
                    out.collect(reference);
                }
            }
        });

        references = allReferences.filter(new FilterFunction<SharedStreetReference>() {
            @Override
            public boolean filter(SharedStreetReference value) throws Exception {
                if(value.formOfWay == SharedStreetReference.FORM_OF_WAY.Other || value.formOfWay == SharedStreetReference.FORM_OF_WAY.Undefined)
                    return false;
                else
                    return true;
            }
        });

        DataSet<Tuple2<UniqueId, SharedStreetGeometry>> geometriesTmp = references.map(new MapFunction<SharedStreetReference, Tuple2<UniqueId, SharedStreetGeometry>>() {
            @Override
            public Tuple2<UniqueId, SharedStreetGeometry> map(SharedStreetReference value) throws Exception {
                return new Tuple2<UniqueId, SharedStreetGeometry>(value.geometry.id, value.geometry);
            }
        });

        geometries = geometriesTmp.distinct(0).map(new MapFunction<Tuple2<UniqueId, SharedStreetGeometry>, SharedStreetGeometry>() {
            @Override
            public SharedStreetGeometry map(Tuple2<UniqueId, SharedStreetGeometry> value) throws Exception {
                return value.f1;
            }
        });



    }

}


