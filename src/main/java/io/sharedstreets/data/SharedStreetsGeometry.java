package io.sharedstreets.data;


import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Polyline;
import com.jsoniter.annotation.JsonIgnore;
import io.sharedstreets.tools.builder.model.BaseSegment;
import io.sharedstreets.tools.builder.util.geo.TileId;
import io.sharedstreets.tools.builder.util.UniqueId;

import java.io.Serializable;


public class SharedStreetsGeometry implements Serializable {


    public UniqueId id;

    public UniqueId startIntersectionId;
    public UniqueId endIntersectionId;
    public UniqueId forwardReferenceId;
    public UniqueId backReferenceId;

    public Geometry geometry;

    @JsonIgnore
    public SharedStreetsOSMMetadata metadata;

    public SharedStreetsGeometry(BaseSegment segment) {

        this.geometry = (Polyline)segment.constructGeometry();

        this.id = SharedStreetsGeometry.generateId(this);

        this.metadata = new SharedStreetsOSMMetadata(this, segment);
    }

    @JsonIgnore
    public TileId getTileKey() {
        if(((Polyline)this.geometry).getPointCount() == 0) {
            return null;
        }
        return TileId.lonLatToTileId(((Polyline)this.geometry).getPoint(0).getX(), ((Polyline)this.geometry).getPoint(0).getY());
    }

    // generate a stable ref
    public static UniqueId generateId(SharedStreetsGeometry ssg) {
        String hashString = new String();

        hashString = "Geometry";

        for(int i = 0; i < ((Polyline)ssg.geometry).getPointCount(); i++) {
            hashString += String.format(" %.6f %.6f", ((Polyline)ssg.geometry).getPoint(i).getX(), ((Polyline)ssg.geometry).getPoint(i).getY());
        }

        return UniqueId.generateHash(hashString);
    }

}
