package io.sharedstreets.tools.builder.transforms;


import io.sharedstreets.data.osm.model.Way;
import io.sharedstreets.tools.builder.model.BaseSegment;
import io.sharedstreets.tools.builder.model.Intersection;
import io.sharedstreets.data.osm.OSMDataStream;
import io.sharedstreets.tools.builder.model.WaySection;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;


public class BaseSegments implements Serializable {

    Logger LOG = LoggerFactory.getLogger(BaseSegments.class);

    public DataSet<BaseSegment> segments;

    public BaseSegments(OSMDataStream dataStream, Intersections intersections) {

        // split ways

        DataSet<Intersection> splittingIntersections = intersections.splittingIntersections();

        // way_id, splitting node_id
        DataSet<Tuple2<Long, Long>> waySplitPoints = splittingIntersections.flatMap(new FlatMapFunction<Intersection, Tuple2<Long, Long>>() {
            public void flatMap(Intersection value, Collector<Tuple2<Long, Long>> out) {
                for (Long wayId : value.intersectingWays) {
                    out.collect(new Tuple2<Long, Long>(wayId, value.nodeId));
                }
            }
        });

        try {
            long splittingIntersectionCount = splittingIntersections.count();
            LOG.info("Splitting intersection count: {}", splittingIntersectionCount);

        } catch (Exception e){
            e.printStackTrace();
        }

        // merging intersections -- we'll use this to filter segments needing to be joined
        DataSet<Intersection> mergingIntersections = intersections.mergingIntersections();

        // map merging intersections by node id
        DataSet<Tuple2<Long, Intersection>> mappedMergingIntersections = mergingIntersections
                .map(new MapFunction<Intersection, Tuple2<Long, Intersection>>() {
                    @Override
                    public Tuple2<Long, Intersection> map(Intersection value) throws Exception {
                        return new Tuple2<>(value.nodeId, value);
                    }
                });

        try {
            long mergingIntersectionCount = mergingIntersections.count();
            LOG.info("Merging intersection count: {}", mergingIntersectionCount);

        } catch (Exception e){
            e.printStackTrace();
        }

        // way_id,  splitting node_id[]
        DataSet<Tuple2<Long, Long[]>> groupedSplitPoints = waySplitPoints.groupBy(0)
                .reduceGroup(new GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long[]>>() {
                @Override
                public void reduce(Iterable<Tuple2<Long, Long>> values,
                                   Collector<Tuple2<Long, Long[]>> out) throws Exception {

                    ArrayList<Long> splitPoints = new ArrayList<>();

                    long id = -1;

                    for(Tuple2<Long, Long> v : values) {

                        id = v.f0;
                        splitPoints.add(v.f1);
                    }

                    Long[] elements = new Long[splitPoints.size()];
                    elements = splitPoints.toArray(elements);

                    Tuple2<Long, Long[]> grouped = new Tuple2<>();

                    grouped.f0 = id;
                    grouped.f1 = elements;

                    out.collect(grouped);
                }
        });

        // can't use key selector on leftinnerjoin (appears to be a flink bug) so mapping to index by id
        DataSet<Tuple2<Long, Way>> mappedWays =dataStream.ways.map(new MapFunction<Way, Tuple2<Long, Way>>() {
            @Override
            public Tuple2<Long, Way> map(Way value) throws Exception {
                return new Tuple2<Long, Way>(value.id, value);
            }
        });

        // convert all ways to way sections based on split points
        DataSet<WaySection> waySections = mappedWays.leftOuterJoin(groupedSplitPoints)
                .where(0)
                .equalTo(0)
                .with(new FlatJoinFunction<Tuple2<Long, Way>, Tuple2<Long, Long[]>, WaySection>() {
                    @Override
                    public void join(Tuple2<Long, Way> way, Tuple2<Long, Long[]> second, Collector<WaySection> out) throws Exception {

                        // unsplit way -- copy everything
                        if(second == null || second.f1.length == 0) {

                            WaySection section = new WaySection();
                            section.wayId  = way.f1.id;
                            section.nodes = Arrays.copyOfRange(way.f1.nodes, 0, way.f1.nodes.length);
                            section.oneWay = way.f1.isOneWay();
                            section.roadClass = way.f1.roadClass();
                            section.link = way.f1.isLink();
                            section.roundabout = way.f1.isRoundabout();
                            out.collect(section);
                        }
                        else {

                            HashSet<Long> splitPoints = new HashSet<>();

                            for(int i = 0; i < second.f1.length; i++) {
                                splitPoints.add(second.f1[i]);
                            }

                            // iterate through node points and split at intersections
                            // split way sections share the split point node
                            int previousSplit = 0;
                            for(int i = 0; i < way.f1.nodes.length; i++) {
                                if(splitPoints.contains(way.f1.nodes[i].nodeId)) {
                                    if(i > previousSplit) {
                                        WaySection section = new WaySection();
                                        section.wayId  = way.f1.id;
                                        section.nodes = Arrays.copyOfRange(way.f1.nodes, previousSplit, i + 1);
                                        section.oneWay = way.f1.isOneWay();
                                        section.roadClass = way.f1.roadClass();
                                        section.link = way.f1.isLink();
                                        section.roundabout = way.f1.isRoundabout();
                                        out.collect(section);

                                        previousSplit = i;
                                    }
                                }
                            }

                            // add remaining points to final segment
                            if(previousSplit < way.f1.nodes.length - 1) {
                                WaySection section = new WaySection();
                                section.wayId = way.f1.id;
                                section.nodes = Arrays.copyOfRange(way.f1.nodes, previousSplit, way.f1.nodes.length);
                                section.oneWay = way.f1.isOneWay();
                                section.roadClass = way.f1.roadClass();
                                section.link = way.f1.isLink();
                                section.roundabout = way.f1.isRoundabout();

                                out.collect(section);
                            }
                        }
                    }
                });


        try {
            long waySectionsCount = waySections.count();
            LOG.info("Way section count: {}", waySectionsCount);

        } catch (Exception e){
            e.printStackTrace();
        }

        // map way section to segments (one section per segment)
        DataSet<BaseSegment> initialSegments = waySections.map(new MapFunction<WaySection, BaseSegment>() {
            @Override
            public BaseSegment map(WaySection value) throws Exception {
                return new BaseSegment(value);
            }
        });


        try {
            long initialSegmentCount = initialSegments.count();
            LOG.info("Way section count: {}", initialSegmentCount);

        } catch (Exception e){
            e.printStackTrace();
        }

        // begin iteration
        // to reduce segments down so all merged sections are contained within a single BaseSegment
        IterativeDataSet<BaseSegment> iterateSegments = initialSegments.iterate(1000);

        // index segments by common start/end nodes -- every segment gets mapped twice once for start/end nodes
        DataSet<Tuple2<Long, BaseSegment>> mappedSegments = iterateSegments.flatMap(new FlatMapFunction<BaseSegment, Tuple2<Long, BaseSegment>>() {
            @Override
            public void flatMap(BaseSegment value, Collector<Tuple2<Long, BaseSegment>> out) throws Exception {

                Tuple2<Long, BaseSegment> firstNode = new Tuple2<>();
                firstNode.f0 = value.getFirstNode();
                firstNode.f1 = value;

                out.collect(firstNode);

                Tuple2<Long, BaseSegment> lastNode = new Tuple2<>();
                lastNode.f0 = value.getLastNode();
                lastNode.f1 = value;

                out.collect(lastNode);
            }
        });


        // join segments with merging intersections (only segments with common merging intersections get combined)
        DataSet<Tuple2<Tuple2<Long, BaseSegment>, Intersection>> joinedSegments = mappedSegments
            .leftOuterJoin(mappedMergingIntersections)
            .where(0)
            .equalTo(0)
            .with(new JoinFunction<Tuple2<Long,BaseSegment>, Tuple2<Long,Intersection>, Tuple2<Tuple2<Long,BaseSegment>, Intersection>>() {
                @Override
                public Tuple2<Tuple2<Long, BaseSegment>, Intersection> join(Tuple2<Long, BaseSegment> first, Tuple2<Long, Intersection> second) throws Exception {
                    if(second != null)
                        return new Tuple2<Tuple2<Long, BaseSegment>, Intersection>(first, second.f1);
                    else
                        return new Tuple2<Tuple2<Long, BaseSegment>, Intersection>(first, null);
                }

            });

        // filter list of segments only to include those with merging intersections, strip off intersection
        DataSet<Tuple2<Long, BaseSegment>> segmentsWithMergingIntersection = joinedSegments
                .flatMap(new FlatMapFunction<Tuple2<Tuple2<Long, BaseSegment>, Intersection>, Tuple2<Long, BaseSegment>>() {
            @Override
            public void flatMap(Tuple2<Tuple2<Long, BaseSegment>, Intersection> value, Collector<Tuple2<Long, BaseSegment>> out) throws Exception {
                // segments without merging intersection will be null from left outerjoin
                if(value.f1 != null && value.f0 != null){
                    out.collect(value.f0);
                }
            }
        });

        // filter list of segments without merging intersections -- need to filter duplictes and merge back with joined segments
        DataSet<Tuple2<Long, BaseSegment>> segmentsWithoutMergingIntersection = joinedSegments.flatMap(new FlatMapFunction<Tuple2<Tuple2<Long, BaseSegment>, Intersection>, Tuple2<Long, BaseSegment>>() {
            @Override
            public void flatMap(Tuple2<Tuple2<Long, BaseSegment>, Intersection> value, Collector<Tuple2<Long, BaseSegment>> out) throws Exception {
                // segments without merging intersection will be null from left outerjoin
                if(value.f1 == null && value.f0 != null){
                    out.collect(new Tuple2<Long, BaseSegment>(value.f0.f1.id, value.f0.f1));
                }
            }
        });

        // remove duplicates
        DataSet<BaseSegment> deduplicatedSegmentsWithoutMergingIntersection = segmentsWithoutMergingIntersection
                .groupBy(0)
                .first(1).map(new MapFunction<Tuple2<Long, BaseSegment>, BaseSegment>() {
                    @Override
                    public BaseSegment map(Tuple2<Long, BaseSegment> value) throws Exception {
                        return value.f1;
                    }
                });


        // combine adjoining/mergable segments into a single segment
        DataSet<BaseSegment> mergedSegments = segmentsWithMergingIntersection
            .groupBy(0)
            .reduceGroup(new GroupReduceFunction<Tuple2<Long, BaseSegment>, BaseSegment>() {
                @Override
                public void reduce(Iterable<Tuple2<Long, BaseSegment>> values, Collector<BaseSegment> out) throws Exception {

                    // by definition should be no more than two segments per shared node
                    int count = 0;
                    BaseSegment baseSegment1 = null;
                    BaseSegment baseSegment2 = null;
                    for(Tuple2<Long, BaseSegment> segment : values) {
                        count++;

                        if(count == 1)
                            baseSegment1 = segment.f1;
                        else if(count == 2)
                            baseSegment2 = segment.f1;
                    }
                    // only emmit if we found two valid segments
                    // TODO figure out error handling inside Flink operator -- no clear reason why segments should not be mergable at this stage but they'll disappear if something goes wrong here
                    if(count == 2 && baseSegment1 != null && baseSegment2 != null) {
                        BaseSegment mergedBaseSegment = BaseSegment.merge(baseSegment1, baseSegment2);
                        if (mergedBaseSegment != null)
                            out.collect(baseSegment1);
                    }

                }
            });

        // need to recount remaining unmerged segments to decide when to stop iteratively merging
        // index segments by common start/end nodes -- every segment gets mapped twice once for start/end nodes
        DataSet<Tuple2<Long, BaseSegment>> remainingMappedSegments = mergedSegments.flatMap(new FlatMapFunction<BaseSegment, Tuple2<Long, BaseSegment>>() {
            @Override
            public void flatMap(BaseSegment value, Collector<Tuple2<Long, BaseSegment>> out) throws Exception {

                Tuple2<Long, BaseSegment> firstNode = new Tuple2<>();
                firstNode.f0 = value.getFirstNode();
                firstNode.f1 = value;

                out.collect(firstNode);

                Tuple2<Long, BaseSegment> lastNode = new Tuple2<>();
                lastNode.f0 = value.getLastNode();
                lastNode.f1 = value;

                out.collect(lastNode);
            }
        });

        // join segments with merging intersections (only segments with common merging intersections get combined)
        DataSet<Tuple2<Tuple2<Long, BaseSegment>, Intersection>> remainingJoinedSegments = remainingMappedSegments
                .leftOuterJoin(mappedMergingIntersections)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Long,BaseSegment>, Tuple2<Long,Intersection>, Tuple2<Tuple2<Long,BaseSegment>, Intersection>>() {
                    @Override
                    public Tuple2<Tuple2<Long, BaseSegment>, Intersection> join(Tuple2<Long, BaseSegment> first, Tuple2<Long, Intersection> second) throws Exception {
                        return new Tuple2<Tuple2<Long, BaseSegment>, Intersection>(first, second.f1);
                    }

                });

        // filter list of segments only to include those with merging intersections, strip off intersection
        DataSet<Tuple2<Long, BaseSegment>> remainingSegmentsWithMergingIntersection = remainingJoinedSegments.flatMap(new FlatMapFunction<Tuple2<Tuple2<Long, BaseSegment>, Intersection>, Tuple2<Long, BaseSegment>>() {
            @Override
            public void flatMap(Tuple2<Tuple2<Long, BaseSegment>, Intersection> value, Collector<Tuple2<Long, BaseSegment>> out) throws Exception {
                // segments without merging intersection will be null from left outerjoin
                if(value.f1 != null && value.f0 != null){
                    out.collect(value.f0);
                }
            }
        });

        // combine merged segments with already complete segements
        DataSet<BaseSegment> iterationOutput = mergedSegments.union(deduplicatedSegmentsWithoutMergingIntersection);

        segments = iterateSegments.closeWith(iterationOutput, remainingSegmentsWithMergingIntersection);

        // phew -- and we've got segments...

    }

}
