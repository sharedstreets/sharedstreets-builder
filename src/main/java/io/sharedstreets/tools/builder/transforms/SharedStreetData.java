package io.sharedstreets.tools.builder.transforms;


import com.esri.core.geometry.Polyline;
import io.sharedstreets.data.SharedStreetsGeometry;
import io.sharedstreets.data.SharedStreetsReference;
import io.sharedstreets.data.SharedStreetsIntersection;
import io.sharedstreets.tools.builder.model.BaseSegment;
import io.sharedstreets.tools.builder.tiles.TilableData;
import io.sharedstreets.tools.builder.util.UniqueId;
import io.sharedstreets.tools.builder.util.geo.TileId;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SharedStreetData implements Serializable {

    public DataSet<SharedStreetsReference> references;
    public DataSet<SharedStreetsIntersection> intersections;
    public DataSet<SharedStreetsGeometry> geometries;

    public SharedStreetData(BaseSegments baseSgments) {


        // Build SharedStreetData references from segments

        references = baseSgments.segments.flatMap(new FlatMapFunction<BaseSegment, SharedStreetsReference>() {
            @Override
            public void flatMap(BaseSegment value, Collector<SharedStreetsReference> out) throws Exception {
                List<SharedStreetsReference> references = SharedStreetsReference.getSharedStreetsReferences(value);

                for (SharedStreetsReference reference : references) {
                    out.collect(reference);
                }
            }
        });

        // map references by intersection ids

        DataSet<Tuple2<SharedStreetsIntersection, SharedStreetsReference>> referencesByIntersection = references.flatMap(new FlatMapFunction<SharedStreetsReference, Tuple2<SharedStreetsIntersection, SharedStreetsReference>>() {
            @Override
            public void flatMap(SharedStreetsReference value, Collector<Tuple2<SharedStreetsIntersection, SharedStreetsReference>> out) throws Exception {

                Tuple2<SharedStreetsIntersection, SharedStreetsReference> startIntersection = new Tuple2<SharedStreetsIntersection, SharedStreetsReference>(value.locationReferences[0].intersection, value);
                out.collect(startIntersection);

                Tuple2<SharedStreetsIntersection, SharedStreetsReference> endIntersection = new Tuple2<SharedStreetsIntersection, SharedStreetsReference>(value.locationReferences[value.locationReferences.length-1].intersection, value);
                out.collect(endIntersection);

            }
        });

        // merge intersection references

        intersections = referencesByIntersection.groupBy(new KeySelector<Tuple2<SharedStreetsIntersection,SharedStreetsReference>, UniqueId>() {
            @Override
            public UniqueId getKey(Tuple2<SharedStreetsIntersection, SharedStreetsReference> value) throws Exception {
                return value.f0.id;
            }
        }).reduceGroup(new GroupReduceFunction<Tuple2<SharedStreetsIntersection, SharedStreetsReference>, SharedStreetsIntersection>() {
            @Override
            public void reduce(Iterable<Tuple2<SharedStreetsIntersection, SharedStreetsReference>> values, Collector<SharedStreetsIntersection> out) throws Exception {
                SharedStreetsIntersection mergedIntersection = null;

                ArrayList<UniqueId> outboundReferences = new ArrayList<>();
                ArrayList<UniqueId> inboundReferences = new ArrayList<>();

                for(Tuple2<SharedStreetsIntersection, SharedStreetsReference> item : values) {

                    if(mergedIntersection == null)
                        mergedIntersection = item.f0;



                    if(mergedIntersection.id.equals(item.f1.locationReferences[0].intersection.id))
                        outboundReferences.add(item.f1.id);
                    else
                        inboundReferences.add(item.f1.id);
                }

                mergedIntersection.outboundSegmentIds = outboundReferences.toArray(new UniqueId[outboundReferences.size()]);
                mergedIntersection.inboundSegmentIds = inboundReferences.toArray(new UniqueId[inboundReferences.size()]);

                out.collect(mergedIntersection);
            }
        });


        // get distinct geometries from reference

        DataSet<Tuple2<UniqueId, SharedStreetsGeometry>> unfilteredGeometries = references
                .map(new MapFunction<SharedStreetsReference, Tuple2<UniqueId, SharedStreetsGeometry>>() {
            @Override
            public Tuple2<UniqueId, SharedStreetsGeometry> map(SharedStreetsReference value) throws Exception {
                return new Tuple2<UniqueId, SharedStreetsGeometry>(value.geometry.id, value.geometry);
            }
        });

        geometries = unfilteredGeometries.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<UniqueId, SharedStreetsGeometry>, SharedStreetsGeometry>() {
            @Override
            public void reduce(Iterable<Tuple2<UniqueId, SharedStreetsGeometry>> values, Collector<SharedStreetsGeometry> out) throws Exception {
                for(Tuple2<UniqueId, SharedStreetsGeometry> value : values){
                    out.collect(value.f1);
                    break;
                }
            }
        });

    }

    public DataSet<Tuple2<TileId, TilableData>> getTiledGeometries(int zLevel)
    {
        DataSet<Tuple2<TileId, TilableData>> data = this.geometries.flatMap(new FlatMapFunction<SharedStreetsGeometry, Tuple2<TileId, TilableData>>() {
            @Override
            public void flatMap(SharedStreetsGeometry value, Collector<Tuple2<TileId, TilableData>> out) throws Exception {
                Set<TileId> tileIds = value.getTileKeys(zLevel);

                for(TileId id : tileIds) {
                    out.collect(new Tuple2<TileId, TilableData>(id, value));
                }
            }
        });

        return data;
    }

    public DataSet<Tuple2<TileId, TilableData>> getTiledReferences(int zLevel)
    {
        DataSet<Tuple2<TileId, TilableData>> data = this.references.flatMap(new FlatMapFunction<SharedStreetsReference, Tuple2<TileId, TilableData>>() {
            @Override
            public void flatMap(SharedStreetsReference value, Collector<Tuple2<TileId, TilableData>> out) throws Exception {
                Set<TileId> tileIds = value.getTileKeys(zLevel);

                for(TileId id : tileIds) {
                    out.collect(new Tuple2<TileId, TilableData>(id, value));
                }
            }
        });

        return data;
    }

    public DataSet<Tuple2<TileId, TilableData>> getTiledIntersections(int zLevel)
    {
        DataSet<Tuple2<TileId, TilableData>> data = this.intersections.flatMap(new FlatMapFunction<SharedStreetsIntersection, Tuple2<TileId, TilableData>>() {
            @Override
            public void flatMap(SharedStreetsIntersection value, Collector<Tuple2<TileId, TilableData>> out) throws Exception {
                Set<TileId> tileIds = value.getTileKeys(zLevel);

                for(TileId id : tileIds) {
                    out.collect(new Tuple2<TileId, TilableData>(id, value));
                }
            }
        });

        return data;
    }

    public DataSet<Tuple2<TileId, TilableData>> mergedData(int zLevel) {

        DataSet<Tuple2<TileId, TilableData>> intersections = getTiledIntersections(zLevel);
        DataSet<Tuple2<TileId, TilableData>> geometries = getTiledGeometries(zLevel);
        DataSet<Tuple2<TileId, TilableData>> references = getTiledReferences(zLevel);

        DataSet<Tuple2<TileId, TilableData>>  mergedData = intersections.union(geometries).union(references);

        return mergedData.partitionByHash(0);
    }

}