package io.sharedstreets.tools.builder.osm;


import com.sun.org.apache.xpath.internal.operations.Bool;
import io.sharedstreets.tools.builder.osm.inputs.OSMPBFNodeInputFormat;
import io.sharedstreets.tools.builder.osm.inputs.OSMPBFRelationInputFormat;
import io.sharedstreets.tools.builder.osm.inputs.OSMPBFWayInputFormat;
import io.sharedstreets.tools.builder.osm.model.*;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class OSMDataStream {

    private String inputFile;
    private ExecutionEnvironment env;

    // input stream
    private OSMPBFNodeInputFormat inputNodes;
    private OSMPBFWayInputFormat inputWays;
    private OSMPBFRelationInputFormat inputRelations;

    // nodes
    public DataSet<NodeEntity> nodes;
    public DataSet<Tuple3<Long, Double, Double>> nodePositions; // node_id, x, y

    // ways
    private DataSet<WayEntity> rawWays;
    public DataSet<Tuple4<Long, Long, Integer, Boolean>> orderedWayNodeLink; // way_id, node_id, order, terminal_point
    public DataSet<Way> ways;

    // relaitons
    public DataSet<Relation> relations;


    public OSMDataStream(String inputFile, ExecutionEnvironment env) {

        this.inputFile = inputFile;
        this.env = env;

        // create input streams

        // node inputs
        inputNodes = new OSMPBFNodeInputFormat();
        inputNodes.setFilePath(this.inputFile);
        nodes = env.createInput(inputNodes, new GenericTypeInfo<NodeEntity>(NodeEntity.class));

        // way inputs
        inputWays = new OSMPBFWayInputFormat();
        inputWays.setFilePath(this.inputFile);
        rawWays = env.createInput(inputWays, new GenericTypeInfo<WayEntity>(WayEntity.class)).partitionByHash(new KeySelector<WayEntity, Long >() {
            @Override
            public Long getKey(WayEntity value) throws Exception {
                return value.id;
            }
        });

        // relation inputs
        inputRelations = new OSMPBFRelationInputFormat();
        inputRelations.setFilePath(this.inputFile);
        relations = env.createInput(inputRelations, new GenericTypeInfo<Relation>(Relation.class));



        buildNodes();

        buildWays();

        // skipping relations for now
        //buildRelations();

    }

    private void buildNodes() {

        // get only the positions of the nodes
        nodePositions = nodes
                .map(new RichMapFunction<NodeEntity, Tuple3<Long, Double, Double>>() {
                    private Counter counter;

                    @Override
                    public void open(Configuration config) {
                        this.counter = getRuntimeContext()
                                .getMetricGroup()
                                .addGroup("OSM")
                                .counter("Nodes");
                    }

                    @Override
                    public Tuple3< Long, Double, Double> map(NodeEntity value) throws Exception {
                        counter.inc();
                        return new Tuple3<Long, Double, Double>(value.id, value.y, value.x);
                    }
                });

    }

    private void buildWays() {

        // filter out all ways without "highway=" tag
        DataSet<WayEntity> filteredWays = rawWays.filter(new RichFilterFunction<WayEntity>() {

            private Counter counter;

            @Override
            public void open(Configuration config) {
                this.counter = getRuntimeContext()
                        .getMetricGroup()
                        .addGroup("OSM")
                        .counter("Ways");
            }

            @Override
            public boolean filter(WayEntity value) throws Exception {

                if (value == null)
                    return false;

                assert value != null;

                if(value.isHighway()) {
                    counter.inc();
                    return true;
                }
                else
                    return false;
            }
        });

        // link way id to node ids with ordering
        // way_id, node_id, order, terminating
        DataSet<Tuple4<Long, Long, Integer, Boolean>> unfilteredOrderedWayNodeLink = filteredWays
                .flatMap(new FlatMapFunction<WayEntity, Tuple4<Long, Long, Integer, Boolean>>() {
                    @Override
                    public void flatMap(WayEntity value, Collector<Tuple4<Long, Long, Integer, Boolean>> out) throws Exception {

                        if (value.relatedObjects != null) {
                            int c = 0;
                            int max = value.relatedObjects.length - 1;
                            for (RelatedObject r : value.relatedObjects) {
                                boolean terminal_point = false;
                                if (c == 0 || c == max)
                                    terminal_point = true;

                                out.collect(new Tuple4<Long, Long, Integer, Boolean>(value.id, r.relatedId, c++, terminal_point));
                            }
                        }
                    }
                }).partitionByHash(0);
        
        // join ways with node positions
        // way_id, order, node_id, lat, lon
        DataSet<Tuple5<Long, Integer, Long, Double, Double>> joinedWaysWithPoints = unfilteredOrderedWayNodeLink
                .joinWithHuge(nodePositions)
                .where(1)
                .equalTo(0).map(new MapFunction<Tuple2<Tuple4<Long, Long, Integer, Boolean>, Tuple3<Long, Double, Double>>, Tuple5<Long, Integer, Long, Double, Double>>() {
                    @Override
                    public Tuple5<Long, Integer, Long, Double, Double> map(Tuple2<Tuple4<Long, Long, Integer, Boolean>, Tuple3<Long, Double, Double>> value) throws Exception {

                        return new Tuple5<Long, Integer, Long, Double, Double>(value.f0.f0, value.f0.f2, value.f1.f0, value.f1.f1, value.f1.f2);
                    }
                }).partitionByHash(0);

        // group nodes by way id and sort on node field order
        // way_id, NodePosition[]
        DataSet<Tuple2<Long, NodePosition[]>> wayNodes = joinedWaysWithPoints
                .groupBy(0)
                .sortGroup(1, Order.ASCENDING)
                .reduceGroup(new GroupReduceFunction<Tuple5<Long, Integer, Long, Double, Double>, Tuple2<Long, NodePosition[]>>() {
                    @Override
                    public void reduce(Iterable<Tuple5<Long, Integer, Long, Double, Double>> values,
                                       Collector<Tuple2<Long, NodePosition[]>> out) throws Exception {
                        long id = -1;

                        ArrayList<NodePosition> positionsArray = new ArrayList<>();

                        for (Tuple5<Long, Integer, Long, Double, Double> t : values) {
                            id = t.f0;
                            NodePosition p = new NodePosition();
                            p.nodeId = t.f2;
                            p.lat = t.f3;
                            p.lon = t.f4;
                            positionsArray.add(p);
                        }

                        NodePosition[] elements = new NodePosition[positionsArray.size()];
                        elements = positionsArray.toArray(elements);

                        out.collect(new Tuple2<Long, NodePosition[]>(id, elements));

                    }
                }).partitionByHash(0);


        // create the way entities
        DataSet<Tuple2<Long, Way>> unfilteredWays = rawWays.joinWithHuge(wayNodes)
                .where(new KeySelector<WayEntity, Long>() {
                    @Override
                    public Long getKey(WayEntity value) throws Exception {
                        return value.id;
                    }
                }).equalTo(0)
                .with(new FlatJoinFunction<WayEntity, Tuple2<Long, NodePosition[]>, Tuple2<Long, Way>>() {
                    @Override
                    public void join(WayEntity first, Tuple2<Long, NodePosition[]> second, Collector<Tuple2<Long, Way>> out)
                            throws Exception {

                        if (first == null) {
                            return;
                        }

                        // take only the ways with attributes ??
                        if (first.fields == null || first.fields.size() == 0) {
                            return;
                        }

                        // skip single-node ways
                        if (second.f1 == null || second.f1.length < 2) {
                            return;
                        }


                        Way way = new Way();
                        way.fields = first.fields;
                        way.id = first.id;
                        way.nodes = second.f1;

                        if(!way.isHighway())
                            return;

                        if(way.roadClass().getValue() > 7)
                            return;

                        out.collect(new Tuple2<>(way.id, way));
                    }
                }).partitionByHash(0);

        orderedWayNodeLink = unfilteredOrderedWayNodeLink
                .leftOuterJoin(unfilteredWays)
                .where(0)
                .equalTo(0)
                .with(new FlatJoinFunction<Tuple4<Long, Long, Integer, Boolean>, Tuple2<Long, Way>, Tuple4<Long, Long, Integer, Boolean>>() {
                    @Override
                    public void join(Tuple4<Long, Long, Integer, Boolean> first, Tuple2<Long, Way> second, Collector<Tuple4<Long, Long, Integer, Boolean>> out) throws Exception {

                        if(second != null)
                            out.collect(first);
                    }
                });

        ways = unfilteredWays.map(new MapFunction<Tuple2<Long, Way>, Way>() {
            @Override
            public Way map(Tuple2<Long, Way> value) throws Exception {
                return value.f1;
            }
        });

    }


    private void buildRelations() {

        DataSet<Relation> retRelations = relations.filter((FilterFunction<Relation>) value -> {

            if (value == null || value.relatedObjects == null)
                return false;

            assert value != null;
            for (RelatedObject r : value.relatedObjects) {
                if (("inner".equals(r.role) || "outer".equals(r.role)) && "way".equals(r.type)) {
                    return false;
                }
            }

            return true;
        });
    }









}
