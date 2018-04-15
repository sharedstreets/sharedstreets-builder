# SharedStreets Builder

The SharedStreets Builder application converts OpenStreetMap data to [SharedStreets protocol buffer tiles](https://github.com/sharedstreets/sharedstreets-ref-system).

SharedStreets uses this tool to generate and maintain a complete global OSM-dervied tile set. Users can operate the tool directly on their OSM or use a pregenerated global tileset provided by SharedStreets.

Support for non-OSM data sources has been moved to the [sharedstreets-conflator](https://github.com/sharedstreets/sharedstreets-conflator) tool.

**Example use**

`java -jar ./sharedstreets-builder-0.1-preview.jar --input data/[osm_input_file].pbf --output ./[tile_output_directory]
`

**Notes**

The builder application is built on Apache Flink. If memory requirements exceed available space, Flink uses a disk-based cache for processing. Processing large OSM data sets may require several hundred gigabytes of free disk space. 
 

**Roadmap**

- [*v0.1:*](https://github.com/sharedstreets/sharedstreets-builder/releases/tag/0.1-preview) OSM support
- *v0.2:* Add OSM metadata support for support ways per [#9](https://github.com/sharedstreets/sharedstreets-builder/issues/9)
- [*v0.3:*]() add heirarchical filterfing for roadClass per [sharedstreets-ref-system/#20](https://github.com/sharedstreets/sharedstreets-ref-system/issues/20#issuecomment-381010861)
