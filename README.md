# SharedStreets Builder

The SharedStreets Builder application converts OpenStreetMap data to [SharedStreets protocol buffer tiles](https://github.com/sharedstreets/sharedstreets-ref-system).

SharedStreets uses this tool to generate and maintain a complete global tile set. Users can operate the tool directly on their OSM or GIS data to develop references for internally maintained data sets.

**Example use**

`java -jar ./sharedstreets-builder-0.1-preview.jar --input data/[osm_input_file].pbf --output ./[tile_output_directory]
`

**Notes**

The builder application is built on Apache Flink. If memory requirements exceed available space, Flink uses a disk-based cache for processing. Processing large OSM data sets may require several hundred gigabytes of free disk space. 

 

**Roadmap**

- [*v0.1:*](https://github.com/sharedstreets/sharedstreets-builder/releases/tag/0.1-preview) OSM support
- *v0.2:* Shapefile support 
- *v0.3:* Compute SharedStreets reference deltas between various input sources.