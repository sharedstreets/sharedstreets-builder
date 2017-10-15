package io.sharedstreets.tools.builder.osm.tools;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class MapStringTools {

    public static String convertToString(Map<String, String> attributes) {
        if (attributes == null)
            return "";

        StringBuilder sb = new StringBuilder();
        for (Entry<String, String> e : attributes.entrySet()) {
            if (sb.length() > 0)
                sb.append('|');
            sb.append(e.getKey()).append("=").append(e.getValue() == null ? "" : e.getValue());
        }
        return sb.toString();
    }

    public static Map<String, String> fromString(String fieldsValuePairs) {
        if (fieldsValuePairs == null)
            return null;

        if (fieldsValuePairs.trim().isEmpty())
            return null;

        Map<String, String> fields = new HashMap<>();

        String[] pairs = fieldsValuePairs.split("\\|");
        for (String s : pairs) {
            if (s.isEmpty())
                continue;

            int idx = s.indexOf('=');
            if (idx == -1)
                continue;

            String fn = s.substring(0, idx);
            String v = s.substring(idx + 1);
            fields.put(fn, v);

        }

        return  fields;

    }

}