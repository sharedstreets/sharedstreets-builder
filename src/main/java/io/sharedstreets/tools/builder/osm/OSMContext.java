package io.sharedstreets.tools.builder.osm;

import org.openstreetmap.osmosis.osmbinary.Osmformat;

import java.io.Serializable;
import java.util.Date;


public class OSMContext implements Serializable {

    protected int granularity;
    protected long lat_offset;
    protected long lon_offset;
    protected int date_granularity;
    protected String strings[];

    public OSMContext(int granularity, long lat_offset, long lon_offset,
                      int date_granular, String[] strings) {

        this.granularity = granularity;
        this.lat_offset = lat_offset;
        this.lon_offset = lon_offset;
        this.date_granularity = date_granular;
        this.strings = strings;
    }

    /**
     * Convert a latitude value stored in a protobuf into a double, compensating
     * for granularity and latitude offset
     */
    public double parseLat(long degree) {
        // Support non-zero offsets. (We don't currently generate them)
        return (granularity * degree + lat_offset) * .000000001;
    }

    /**
     * Convert a longitude value stored in a protobuf into a double,
     * compensating for granularity and longitude offset
     */
    public double parseLon(long degree) {
        // Support non-zero offsets. (We don't currently generate them)
        return (granularity * degree + lon_offset) * .000000001;
    }

    /** Take a Info protocol buffer containing a date and convert it into a java Date object */
    public Date getDate(Osmformat.Info info) {
        if (info.hasTimestamp()) {
            return new Date(date_granularity * (long) info.getTimestamp());
        } else
            return NODATE;
    }
    public static final Date NODATE = new Date(-1);

    /** Get a string based on the index used.
     *
     * Index 0 is reserved to use as a delimiter, therefore, index 1 corresponds to the first string in the table
     * @param id
     * @return
     */
    public String getStringById(int id) {
        return strings[id];
    }

    public int getStringLength()
    {
        return strings.length;
    }


}