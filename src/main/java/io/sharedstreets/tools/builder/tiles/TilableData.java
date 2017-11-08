package io.sharedstreets.tools.builder.tiles;


import io.sharedstreets.tools.builder.util.geo.TileId;

import java.util.Set;

public abstract class TilableData {

    public abstract String getId();

    public abstract Set<TileId> getTileKeys(int zLevel);

}
