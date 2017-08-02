package io.opentraffic.osmlr.builder.model;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class Intersection implements Serializable {

    private static final long serialVersionUID = 1L;

    public Long nodeId;
    public Set<Long> terminatingWays;
    public Set<Long> intersectingWays;

    public Intersection(){

        terminatingWays = new HashSet<Long>();
        intersectingWays = new HashSet<Long>();

        this.nodeId = nodeId;
    }

    public void addWay(Long wayId, Long nodeId, Boolean terminating_node) {

        if(this.nodeId == null)
            this.nodeId = nodeId;

        assert this.nodeId == nodeId;

        if(terminating_node)
            terminatingWays.add(wayId);
        else
            intersectingWays.add(wayId);
    }

    // node with more than one way
    public boolean isIntersection() {
        return (terminatingWays.size() + intersectingWays.size()) > 1;
    }

    // splits one or more ways
    public boolean isSplitting() {
        return (intersectingWays.size() > 1) || (terminatingWays.size() > 0 && intersectingWays.size() == 1);
    }

    // find co-linear intersections (with no splitting intersections)
    public boolean isMerging() {
        return (terminatingWays.size() == 2  && intersectingWays.size() == 0);
    }
}