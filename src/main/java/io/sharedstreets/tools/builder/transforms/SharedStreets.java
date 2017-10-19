package io.sharedstreets.tools.builder.transforms;


import com.esri.core.geometry.Polyline;
import io.sharedstreets.data.SharedStreetsGeometry;
import io.sharedstreets.data.SharedStreetsReference;
import io.sharedstreets.data.SharedStreetsIntersection;
import io.sharedstreets.tools.builder.model.BaseSegment;
import io.sharedstreets.tools.builder.util.UniqueId;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class SharedStreets implements Serializable {

    public DataSet<SharedStreetsReference> references;
    public DataSet<SharedStreetsIntersection> intersections;
    public DataSet<SharedStreetsGeometry> geometries;

    public SharedStreets(BaseSegments segments) {


        // get list of all references
        DataSet<SharedStreetsReference> allReferences = segments.segments.flatMap(new FlatMapFunction<BaseSegment, SharedStreetsReference>() {
            @Override
            public void flatMap(BaseSegment value, Collector<SharedStreetsReference> out) throws Exception {
                List<SharedStreetsReference> references = SharedStreetsReference.getSharedStreetsReferences(value);

                for (SharedStreetsReference reference : references) {
                    out.collect(reference);
                }
            }
        });

        // build SharedStreetsIntersections from references

        // node_id, street ref
        DataSet<Tuple2<Long, SharedStreetsReference>> referenceIntersectionNodeMap = allReferences
                .flatMap(new FlatMapFunction<SharedStreetsReference, Tuple2<Long, SharedStreetsReference>>() {
                    @Override
                    public void flatMap(SharedStreetsReference value, Collector<Tuple2<Long, SharedStreetsReference>> out) throws Exception {

                        Tuple2<Long, SharedStreetsReference> startIntersectionRef = new Tuple2(value.geometry.metadata.getStartNodeId(), value);
                        out.collect(startIntersectionRef);

                        Tuple2<Long, SharedStreetsReference> endIntersectionRef = new Tuple2(value.geometry.metadata.getEndNodeId(), value);
                        out.collect(endIntersectionRef);
                    }
                });

        DataSet<Tuple3<Long, ArrayList<SharedStreetsReference>, SharedStreetsIntersection>> nodeMappedIntersections = referenceIntersectionNodeMap
                .groupBy(0).
                reduceGroup(new GroupReduceFunction<Tuple2<Long, SharedStreetsReference>, Tuple3<Long, ArrayList<SharedStreetsReference>, SharedStreetsIntersection>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<Long, SharedStreetsReference>> values, Collector<Tuple3<Long, ArrayList<SharedStreetsReference>, SharedStreetsIntersection>> out) throws Exception {
                        SharedStreetsIntersection intersection = new SharedStreetsIntersection();

                        HashSet<UniqueId> inboundSegmentIds = new HashSet<>();
                        HashSet<UniqueId> outboundSegmentIds = new HashSet<>();

                        ArrayList<SharedStreetsReference> references = new ArrayList<>();

                        for (Tuple2<Long, SharedStreetsReference> value : values) {
                            intersection.osmNodeId = value.f0;

                            if(intersection.id == null)
                                intersection.id = SharedStreetsIntersection.generateId(intersection);

                            if (intersection.osmNodeId.equals(value.f1.geometry.metadata.getStartNodeId())) {
                                outboundSegmentIds.add(value.f1.id);
                                value.f1.geometry.startIntersectionId = intersection.id;
                                intersection.geometry = ((Polyline)value.f1.geometry.geometry).getPoint(0);
                            }

                            if (intersection.osmNodeId.equals(value.f1.geometry.metadata.getEndNodeId())) {
                                inboundSegmentIds.add(value.f1.id);
                                value.f1.geometry.endIntersectionId = intersection.id;
                                intersection.geometry = ((Polyline)value.f1.geometry.geometry).getPoint(((Polyline)value.f1.geometry.geometry).getPointCount()- 1);
                            }

                            references.add(value.f1);
                        }

                        intersection.inboundSegmentIds = inboundSegmentIds.toArray(new UniqueId[inboundSegmentIds.size()]);
                        intersection.outboundSegmentIds = outboundSegmentIds.toArray(new UniqueId[outboundSegmentIds.size()]);

                        out.collect(new Tuple3<Long, ArrayList<SharedStreetsReference>, SharedStreetsIntersection>(intersection.osmNodeId, references, intersection));
                    }
                });

        // need to merge each half of the reference (one for to and from) back into a single reference

        DataSet<Tuple2<UniqueId, SharedStreetsReference>> premergedReferences = nodeMappedIntersections.flatMap(new FlatMapFunction<Tuple3<Long, ArrayList<SharedStreetsReference>, SharedStreetsIntersection>, Tuple2<UniqueId, SharedStreetsReference>>() {
            @Override
            public void flatMap(Tuple3<Long, ArrayList<SharedStreetsReference>, SharedStreetsIntersection> value, Collector<Tuple2<UniqueId, SharedStreetsReference>> out) throws Exception {
                for (SharedStreetsReference reference : value.f1) {
                    out.collect(new Tuple2<UniqueId, SharedStreetsReference>(reference.id, reference));
                }
            }
        });

        references = premergedReferences.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<UniqueId, SharedStreetsReference>, SharedStreetsReference>() {
            @Override
            public void reduce(Iterable<Tuple2<UniqueId, SharedStreetsReference>> values, Collector<SharedStreetsReference> out) throws Exception {
                SharedStreetsReference toRef = null;
                SharedStreetsReference fromRef = null;

                for (Tuple2<UniqueId, SharedStreetsReference> value : values) {
                    if (value.f1.geometry.startIntersectionId != null)
                        fromRef = value.f1;
                    else if (value.f1.geometry.endIntersectionId != null)
                        toRef = value.f1;
                }

                // copy start id from fromRef;
                if (toRef != null && fromRef != null) {

                    toRef.geometry.startIntersectionId = fromRef.geometry.startIntersectionId;

                    if (toRef.locationReferences != null && toRef.locationReferences.length >= 2) {

                        if(toRef.id.equals(toRef.geometry.forwardReferenceId)) {
                            toRef.locationReferences[0].intersectionId = toRef.geometry.startIntersectionId;
                            toRef.locationReferences[toRef.locationReferences.length - 1].intersectionId = toRef.geometry.endIntersectionId;
                        }
                        else {
                            toRef.locationReferences[0].intersectionId = toRef.geometry.endIntersectionId;
                            toRef.locationReferences[toRef.locationReferences.length - 1].intersectionId = toRef.geometry.startIntersectionId;

                        }
                    }

                    out.collect(toRef);
                }
            }
        });

        intersections = nodeMappedIntersections.map(new MapFunction<Tuple3<Long, ArrayList<SharedStreetsReference>, SharedStreetsIntersection>, SharedStreetsIntersection>() {
            @Override
            public SharedStreetsIntersection map(Tuple3<Long, ArrayList<SharedStreetsReference>, SharedStreetsIntersection> value) throws Exception {
                return value.f2;
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

}


