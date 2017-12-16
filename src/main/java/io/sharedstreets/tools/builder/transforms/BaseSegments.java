package io.sharedstreets.tools.builder.transforms;


import io.sharedstreets.tools.builder.osm.model.Way;
import io.sharedstreets.tools.builder.model.BaseSegment;
import io.sharedstreets.tools.builder.model.WayIntersection;
import io.sharedstreets.tools.builder.osm.OSMDataStream;
import io.sharedstreets.tools.builder.model.WaySection;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
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

        DataSet<WayIntersection> splittingIntersections = intersections.splittingIntersections();

        // way_id, splitting node_id
        DataSet<Tuple2<Long, Long>> waySplitPoints = splittingIntersections.flatMap(new FlatMapFunction<WayIntersection, Tuple2<Long, Long>>() {
            public void flatMap(WayIntersection value, Collector<Tuple2<Long, Long>> out) {
                for (Long wayId : value.intersectingWays) {
                    out.collect(new Tuple2<Long, Long>(wayId, value.nodeId));
                }
            }
        });

        DataSet<Tuple2<Long, WayIntersection>> mappedSplitingIntersections = splittingIntersections
                .map(new MapFunction<WayIntersection, Tuple2<Long, WayIntersection>>() {
                    @Override
                    public Tuple2<Long, WayIntersection> map(WayIntersection value) throws Exception {
                        return new Tuple2<>(value.nodeId, value);
                    }
                });


        // merging intersections -- we'll use this to filter segments needing to be joined
        DataSet<WayIntersection> mergingIntersections = intersections.mergingIntersections();

        // map merging intersections by node id
        DataSet<Tuple2<Long, WayIntersection>> mappedMergingIntersections = mergingIntersections
                .map(new MapFunction<WayIntersection, Tuple2<Long, WayIntersection>>() {
                    @Override
                    public Tuple2<Long, WayIntersection> map(WayIntersection value) throws Exception {
                        return new Tuple2<>(value.nodeId, value);
                    }
                });


        // way_id,  splitting node_id[]
        DataSet<Tuple2<Long, Long[]>> groupedSplitPoints = waySplitPoints.groupBy(0)
                .reduceGroup(new GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long[]>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<Long, Long>> values,
                                       Collector<Tuple2<Long, Long[]>> out) throws Exception {

                        ArrayList<Long> splitPoints = new ArrayList<>();

                        long id = -1;

                        for (Tuple2<Long, Long> v : values) {

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
        DataSet<Tuple2<Long, Way>> mappedWays = dataStream.ways.map(new MapFunction<Way, Tuple2<Long, Way>>() {
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
                        if (second == null || second.f1.length == 0) {

                            WaySection section = new WaySection();
                            section.wayId = way.f1.id;
                            section.nodes = Arrays.copyOfRange(way.f1.nodes, 0, way.f1.nodes.length);
                            section.oneWay = way.f1.isOneWay();
                            section.roadClass = way.f1.roadClass();
                            section.link = way.f1.isLink();
                            section.roundabout = way.f1.isRoundabout();
                            out.collect(section);
                        } else {

                            HashSet<Long> splitPoints = new HashSet<>();

                            for (int i = 0; i < second.f1.length; i++) {
                                splitPoints.add(second.f1[i]);
                            }

                            // iterate through node points and split at intersections
                            // split way sections share the split point node
                            int previousSplit = 0;
                            for (int i = 0; i < way.f1.nodes.length; i++) {
                                if (splitPoints.contains(way.f1.nodes[i].nodeId)) {
                                    if (i > previousSplit) {
                                        WaySection section = new WaySection();
                                        section.wayId = way.f1.id;
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
                            if (previousSplit < way.f1.nodes.length - 1) {
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


        // map way section to segments (one section per segment)
        DataSet<BaseSegment> initialSegments = waySections.map(new MapFunction<WaySection, BaseSegment>() {
            @Override
            public BaseSegment map(WaySection value) throws Exception {

                return new BaseSegment(value);
            }
        });


        // begin iteration
        // to reduce segments down so all merged sections are contained within a single BaseSegment
        IterativeDataSet<BaseSegment> iterateSegments = initialSegments.iterate(1000);

        //  Input:
        //
        //   * non-merging intersection
        //   0 merging intersection
        //                                           0====G====0
        //            |                              |         |
        //            |                              H         F
        //            |                              |         |
        //  =====A====*====B====0====C=====0====D====*====E====0
        //
        //  Output:
        //                                           ===========
        //            |                              |         |
        //            |                              |   EFHG  |
        //            |                              |         |
        //  =====A====*=============BCD==============*==========
        //

        //
        // Step 1: Link each segment to intersections
        //
        // Input segments
        // =====A====  ====B====  ====C=====  ====D====  ====E=====
        //
        // Link to intersections and
        //
        // Store each segment-intersection relationship in list
        // =====A====*  *====B====  0====C=====  0====D====  *====E=====
        //               ====B====0  ====C=====0  ====D====*
        //

        // index segments by common start/end nodes -- every segment gets mapped twice, once each for start and end nodes
        // Tuple2(intersectionId, segmentId, merging, segment)
        DataSet<Tuple4<Long, Long, Boolean, BaseSegment>> segmentIntersectionMap = iterateSegments
                .flatMap(new FlatMapFunction<BaseSegment, Tuple4<Long, Long, Boolean, BaseSegment>>() {
                    @Override
                    public void flatMap(BaseSegment value, Collector<Tuple4<Long, Long, Boolean, BaseSegment>> out) throws Exception {

                        Tuple4<Long, Long, Boolean, BaseSegment> firstNode = new Tuple4<Long, Long, Boolean, BaseSegment>();
                        firstNode.f0 = value.getFirstNode();
                        firstNode.f1 = value.id;
                        firstNode.f2 = false;
                        firstNode.f3 = value;

                        out.collect(firstNode);

                        Tuple4<Long, Long, Boolean, BaseSegment> lastNode = new Tuple4<Long, Long, Boolean, BaseSegment>();
                        lastNode.f0 = value.getLastNode();
                        lastNode.f1 = value.id;
                        lastNode.f2 = false;
                        lastNode.f3 = value;

                        out.collect(lastNode);
                    }
                });

        // join segments with merging intersections (only segments with common merging intersections get combined)
        DataSet<Tuple4<Long, Long, Boolean, BaseSegment>> unfilteredSegmentWithMergingIntersection = segmentIntersectionMap
                .leftOuterJoin(mappedMergingIntersections)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple4<Long, Long, Boolean, BaseSegment>, Tuple2<Long, WayIntersection>, Tuple4<Long, Long, Boolean, BaseSegment>>() {
                    @Override
                    public Tuple4<Long, Long, Boolean, BaseSegment> join(Tuple4<Long, Long, Boolean, BaseSegment> first, Tuple2<Long, WayIntersection> second) throws Exception {

                        if (second != null && second.f1 != null && second.f1.isMerging())
                            first.f2 = true;
                        else
                            first.f2 = false;

                        return first;
                    }
                });

        // need to clean up merging intersections that can't actually merge (use BaseSegment.canMerge criteria)

        DataSet<Tuple4<Long, Long, Boolean, BaseSegment>> cleanedSegmentsWithMergingIntersection = unfilteredSegmentWithMergingIntersection
                .groupBy(0) // nodeId
                .reduceGroup(new GroupReduceFunction<Tuple4<Long, Long, Boolean, BaseSegment>, Tuple4<Long, Long, Boolean, BaseSegment>>() {
                    @Override
                    public void reduce(Iterable<Tuple4<Long, Long, Boolean, BaseSegment>> values, Collector<Tuple4<Long, Long, Boolean, BaseSegment>> out) throws Exception {
                        ArrayList<Tuple4<Long, Long, Boolean, BaseSegment>> segmentList = new ArrayList<>();

                        for (Tuple4<Long, Long, Boolean, BaseSegment> value : values) {
                            segmentList.add(value);
                        }

                        if (segmentList.size() == 2) {
                            if (BaseSegment.canMerge(segmentList.get(0).f3, segmentList.get(1).f3)) {
                                for (Tuple4<Long, Long, Boolean, BaseSegment> value : segmentList) {
                                    out.collect(value);
                                }
                            } else {
                                for (Tuple4<Long, Long, Boolean, BaseSegment> value : segmentList) {
                                    value.f2 = false;
                                    out.collect(value);
                                }
                            }
                        } else {
                            for (Tuple4<Long, Long, Boolean, BaseSegment> value : segmentList) {
                                value.f2 = false;
                                out.collect(value);
                            }
                        }
                    }
                });


        //
        // Step 2: Remove duplicate sections
        //
        // Only want to process each segment once per iteration. For segments with both merging and non-merging, keep the merging version.
        // For segments with two merging intersections, keep the first segment-intersection relationship
        // (the other intersection will get processed in the next iteration).
        //
        // =====A====*              0====C=====  0====D====  *====E=====
        //              ====B====0
        //

        DataSet<Tuple4<Long, Long, Boolean, BaseSegment>> deduplicatedSegments = cleanedSegmentsWithMergingIntersection
                .groupBy(1) // segmentId
                .reduceGroup(new GroupReduceFunction<Tuple4<Long, Long, Boolean, BaseSegment>, Tuple4<Long, Long, Boolean, BaseSegment>>() {
                    @Override
                    public void reduce(Iterable<Tuple4<Long, Long, Boolean, BaseSegment>> values, Collector<Tuple4<Long, Long, Boolean, BaseSegment>> out) throws Exception {
                        Tuple4<Long, Long, Boolean, BaseSegment> savedSegment = null;


                        for (Tuple4<Long, Long, Boolean, BaseSegment> value : values) {

                            if (value.f2) {
                                // iterate through segments until we find a merging intersection -- keep it
                                savedSegment = value;
                                break;
                            }
                            // alternatively keep one non-merging segment
                            savedSegment = value;
                        }

                        out.collect(savedSegment);
                    }
                });


        //
        // Step 3: Filter segments without merging intersections
        //
        // Non-merging sections (emit as output):
        //  =====A====*  *====E=====
        //
        // Merging sections:
        //              0====C=====  0====D====
        //   ===B====0
        //

        // keep nonmerging segments to merge into final result set
        DataSet<BaseSegment> nonmergingSegments = deduplicatedSegments.flatMap(new FlatMapFunction<Tuple4<Long, Long, Boolean, BaseSegment>, BaseSegment>() {
            @Override
            public void flatMap(Tuple4<Long, Long, Boolean, BaseSegment> value, Collector<BaseSegment> out) throws Exception {

                if (!value.f2)
                    out.collect(value.f3);
            }
        });


        DataSet<Tuple4<Long, Long, Boolean, BaseSegment>> mergingSegments = deduplicatedSegments.flatMap(new FlatMapFunction<Tuple4<Long, Long, Boolean, BaseSegment>, Tuple4<Long, Long, Boolean, BaseSegment>>() {
            @Override
            public void flatMap(Tuple4<Long, Long, Boolean, BaseSegment> value, Collector<Tuple4<Long, Long, Boolean, BaseSegment>> out) throws Exception {

                if (value.f2)
                    out.collect(value);
            }
        });

        //
        // Step 4: Remove loop sections
        //
        // Input ways:
        //
        //           0====G====0
        //           |         |
        //           H         F
        //           |         |
        //  =========*====E====0
        //
        // becomes:
        //
        //   *=======EFGH======*
        //
        //  with both ends referring to same merging node
        //  this creates a non-terminating loop so we remove loop sections as non-merging segments
        //

        // save loop sections to merge into final results
        DataSet<BaseSegment> loopSegments = mergingSegments.flatMap(new FlatMapFunction<Tuple4<Long, Long, Boolean, BaseSegment>, BaseSegment>() {
            @Override
            public void flatMap(Tuple4<Long, Long, Boolean, BaseSegment> value, Collector<BaseSegment> out) throws Exception {

                if (value.f3.getLastNode().equals(value.f3.getFirstNode()))
                    out.collect(value.f3);
            }
        });

        DataSet<Tuple4<Long, Long, Boolean, BaseSegment>> filteredMergedSegements = mergingSegments.filter(new FilterFunction<Tuple4<Long, Long, Boolean, BaseSegment>>() {
            @Override
            public boolean filter(Tuple4<Long, Long, Boolean, BaseSegment> value) throws Exception {

                if (value.f3.getLastNode().equals(value.f3.getFirstNode()))
                    return false;
                else
                    return true;
            }
        });


        //
        // Step 5: Merge sections
        //
        // Merged sections:
        //
        //   =======BC=======  ====D====
        //

        // combine adjoining/mergable segments into a single segment
        DataSet<BaseSegment> mergedSegments = filteredMergedSegements
                .groupBy(0)
                .reduceGroup(new GroupReduceFunction<Tuple4<Long, Long, Boolean, BaseSegment>, BaseSegment>() {
                    @Override
                    public void reduce(Iterable<Tuple4<Long, Long, Boolean, BaseSegment>> values, Collector<BaseSegment> out) throws Exception {

                        // by definition should be no more than two segments per shared node
                        int count = 0;
                        BaseSegment baseSegment1 = null;
                        BaseSegment baseSegment2 = null;

                        for (Tuple4<Long, Long, Boolean, BaseSegment> segmentIntersection : values) {

                            count++;

                            if (count == 1)
                                baseSegment1 = segmentIntersection.f3;
                            else if (count == 2)
                                baseSegment2 = segmentIntersection.f3;
                        }

                        // TODO figure out error handling inside Flink operator -- no clear reason why segments should not be mergable at this stage but they'll disappear if something goes wrong here


                        if (count == 2 && baseSegment1 != null && baseSegment2 != null) {
                            BaseSegment mergedBaseSegment = BaseSegment.merge(baseSegment1, baseSegment2);
                            if (mergedBaseSegment != null)
                                // emmit merged segment if we found two valid segments
                                out.collect(mergedBaseSegment);
                            else
                                System.out.println("Could not merge " + baseSegment1.getWayIds() + " with " + baseSegment2.getWayIds());
                        } else if (count == 1 && baseSegment1 != null) {
                            // emmit unmerged (but mergable) segment for processing in future iteration
                            out.collect(baseSegment1);
                        }

                    }
                });

        // Step 6: Reprocess merged segments (loop until no segments left to merge)

        // recombine nonmerging and loop segments with merged segments
        DataSet<BaseSegment> recombinedSegment = mergedSegments
                .union(nonmergingSegments)
                .union(loopSegments);

        // finalize iteration -- if mergedSegments is empty nothing left to merge.
        segments = iterateSegments.closeWith(recombinedSegment, mergedSegments);


    }

}

