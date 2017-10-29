package io.sharedstreets.data;

import com.esri.core.geometry.Point;
import io.sharedstreets.tools.builder.util.UniqueId;
import io.sharedstreets.tools.builder.util.geo.TileId;

public class SharedStreetsIntersection implements Comparable {

    public UniqueId id;
    public Long osmNodeId;
    public Point point;
    public UniqueId[] inboundSegmentIds;
    public UniqueId[] outboundSegmentIds;

    // TODO this is where turn restrictions go...

    public SharedStreetsIntersection() {

    }

    public TileId getTileKey() {
        return TileId.lonLatToTileId(this.point.getX(), this.point.getY());
    }

    public static UniqueId generateId(SharedStreetsIntersection ssi) {
        String hashString = new String();

        hashString = String.format("Intersection %.6f %.6f", ssi.point.getX(), ssi.point.getY());

        return UniqueId.generateHash(hashString);

    }

    @Override
    public int compareTo(Object o) {
        return this.id.compareTo(o);
    }

    @Override
    public int hashCode() {
        return this.id.hashCode();
    }
}
