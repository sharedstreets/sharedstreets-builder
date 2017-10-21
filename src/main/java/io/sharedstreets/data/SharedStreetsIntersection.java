package io.sharedstreets.data;

import com.esri.core.geometry.Point;
import io.sharedstreets.tools.builder.util.UniqueId;
import io.sharedstreets.tools.builder.util.geo.TileId;

public class SharedStreetsIntersection {

    public UniqueId id;
    public Long osmNodeId;
    public Point geometry;
    public UniqueId[] inboundSegmentIds;
    public UniqueId[] outboundSegmentIds;

    // TODO this is where turn restrictions go...

    public SharedStreetsIntersection() {

    }

    public TileId getTileKey() {
        return TileId.lonLatToTileId(((Point)this.geometry).getX(), ((Point)this.geometry).getY());
    }

    public static UniqueId generateId(SharedStreetsIntersection ssi) {
        String hashString = new String();

        hashString = String.format("%.6f %.6f", ssi.geometry.getX(), ssi.geometry.getX());

        return UniqueId.generateHash(hashString);

    }
}
