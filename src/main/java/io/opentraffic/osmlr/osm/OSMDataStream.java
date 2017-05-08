package io.opentraffic.osmlr.osm;


import com.esri.core.geometry.*;
import io.opentraffic.osmlr.osm.inputs.OSMPBFNodeInputFormat;
import io.opentraffic.osmlr.osm.inputs.OSMPBFRelationInputFormat;
import io.opentraffic.osmlr.osm.inputs.OSMPBFWayInputFormat;
import io.opentraffic.osmlr.osm.model.*;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.util.Collector;

public class OSMDataStream {

    private String inputFile;
    private ExecutionEnvironment env;

    // input stream
    private OSMPBFNodeInputFormat inputNodes;
    private OSMPBFWayInputFormat inputWays;
    private OSMPBFRelationInputFormat inputRelations;

    // nodes
    public DataSet<NodeEntity> nodes;
    public DataSet<NodePosition> nodePositions; // node_id, x, y

    // ways
    private DataSet<WayEntity> rawWays;
    public DataSet<WayNodeLink> orderedWayNodeLink; // way_id, node_id, order, terminal_point
    public DataSet<ComplexEntity> ways;

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
        rawWays = env.createInput(inputWays, new GenericTypeInfo<WayEntity>(WayEntity.class));

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
                .map(new MapFunction<NodeEntity, NodePosition>() {
                    @Override
                    public NodePosition map(NodeEntity value) throws Exception {
                        return new NodePosition(value.id, value.x, value.y);
                    }
                }).sortPartition(new KeySelector<NodePosition, Long>() {
                    @Override
                    public Long getKey(NodePosition nodePosition) {
                        return nodePosition.nodeId;
                    }
                }, Order.ASCENDING);

    }

    private void buildWays() {

        // filter out all ways without "highway=" tag
        DataSet<WayEntity> filteredWays = rawWays.filter(new FilterFunction<WayEntity>() {
            @Override
            public boolean filter(WayEntity value) throws Exception {

                if (value == null)
                    return false;

                assert value != null;

               return value.isHighway();
            }
        });

        // link way id to node ids with ordering
        // way_id, node_id, order
        orderedWayNodeLink = filteredWays
                .flatMap(new FlatMapFunction<WayEntity, WayNodeLink>() {
                    @Override
                    public void flatMap(WayEntity value, Collector<WayNodeLink> out) throws Exception {
                        if (value.relatedObjects != null) {
                            int c = 0;
                            int max = value.relatedObjects.length - 1;
                            for (RelatedObject r : value.relatedObjects) {
                                boolean terminal_point = false;
                                if(c == 0 || c == max)
                                    terminal_point = true;
                                out.collect(new WayNodeLink(value.id, r.relatedId, c++, terminal_point));
                            }
                        }
                    }
                }).sortPartition(new KeySelector<WayNodeLink, Long>() {
                    @Override
                    public Long getKey(WayNodeLink link) {
                        return link.nodeId;
                    }
                }, Order.ASCENDING);


        // join ways with node positions
        // way_id, order, x, y
        DataSet<Tuple4<Long, Integer, Double, Double>> joinedWaysWithPoints = orderedWayNodeLink.joinWithHuge(nodePositions).where(new KeySelector<WayNodeLink, Long>() {
                    @Override
                    public Long getKey(WayNodeLink link) {
                        return link.nodeId;
                    }
                })
                .equalTo(new KeySelector<NodePosition, Long>() {
                    @Override
                    public Long getKey(NodePosition nodePosition) {
                        return nodePosition.nodeId;
                    }
                }).map(new MapFunction<Tuple2<WayNodeLink, NodePosition>, Tuple4<Long, Integer, Double, Double>>() {
                    @Override
                    public Tuple4<Long, Integer, Double, Double> map(Tuple2<WayNodeLink, NodePosition> value) throws Exception {
                        return new Tuple4<>(value.f0.wayId, value.f0.order, value.f1.x, value.f1.y);
                    }
                });

        // group nodes by way id and sort on node field order
        // way_id, esri_polyline
        DataSet<Tuple2<Long, byte[]>> waysGeometry = joinedWaysWithPoints.groupBy(0).sortGroup(1, Order.ASCENDING)
                .reduceGroup(new GroupReduceFunction<Tuple4<Long, Integer, Double, Double>, Tuple2<Long, byte[]>>() {
                    @Override
                    public void reduce(Iterable<Tuple4<Long, Integer, Double, Double>> values,
                                       Collector<Tuple2<Long, byte[]>> out) throws Exception {
                        long id = -1;

                        MultiPath multiPath;

                        multiPath = new Polyline();

                        boolean started = false;

                        for (Tuple4<Long, Integer, Double, Double> t : values) {
                            id = t.getField(0);
                            double x = t.getField(2);
                            double y = t.getField(3);

                            if (!started) {
                                multiPath.startPath(new Point(x, y));
                                started = true;
                            } else {
                                multiPath.lineTo(new Point(x, y));
                            }
                        }

                        byte[] elements = GeometryEngine.geometryToEsriShape(multiPath);

                        out.collect(new Tuple2<Long, byte[]>(id, elements));

                    }
                });

        // create the way entities
        ways = rawWays.joinWithHuge(waysGeometry)
                .where(new KeySelector<WayEntity, Long>() {
                    @Override
                    public Long getKey(WayEntity value) throws Exception {
                        return value.id;
                    }
                }).equalTo(0).with(new FlatJoinFunction<WayEntity, Tuple2<Long, byte[]>, ComplexEntity>() {
                    @Override
                    public void join(WayEntity first, Tuple2<Long, byte[]> second, Collector<ComplexEntity> out)
                            throws Exception {

                        if (first == null) {
                            return;
                        }

                        // take only the ways with attributes ??
                        if (first.fields == null || first.fields.size() == 0) {
                            return;
                        }

                        ComplexEntity ce = new ComplexEntity();
                        ce.fields = first.fields;
                        ce.id = first.id;
                        ce.shapeGeometry = second.f1;
                        ce.geomType = Geometry.Type.Polyline;

                        out.collect(ce);
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
