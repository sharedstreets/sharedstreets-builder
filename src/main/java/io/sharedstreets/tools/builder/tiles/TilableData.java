package io.sharedstreets.tools.builder.tiles;


import io.sharedstreets.tools.builder.util.geo.TileId;

import java.io.IOException;
import java.util.Set;

public abstract class TilableData {

    public abstract String getType();

    public abstract byte[] toBinary() throws IOException;

    public abstract String getId();

    public abstract Set<TileId> getTileKeys(int zLevel);

}
