package io.sharedstreets.tools.builder.tiles;

/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/


import io.sharedstreets.data.SharedStreetsGeometry;
import io.sharedstreets.tools.builder.osm.model.Way;
import io.sharedstreets.tools.builder.util.geo.TileId;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

/**
 * The abstract base class for all Rich output formats that are file based. Contains the logic to
 * open/close the target
 * file streams.
 */
@Public
public class ProtoTileOutputFormat<IT extends  Tuple2<TileId, TilableData>> extends TiledNIOFileOutputFormat<IT> {

    public ProtoTileOutputFormat(String outputPath, Way.ROAD_CLASS filteredClass) {
        super(outputPath, "pbf", filteredClass);
    }

    @Override
    public void writeRecord(IT record) throws IOException {

        this.writeRecord(record.f0, record.f1.getType(), record.f1.toBinary());

        if(record.f1 instanceof SharedStreetsGeometry){
            this.writeRecord(record.f0, ((SharedStreetsGeometry) record.f1).metadata.getType(), ((SharedStreetsGeometry) record.f1).metadata.toBinary());
        }
    }
}