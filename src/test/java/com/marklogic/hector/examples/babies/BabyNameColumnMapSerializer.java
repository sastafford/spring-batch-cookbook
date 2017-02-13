package com.marklogic.hector.examples.babies;

import com.marklogic.hector.XmlStringColumnMapSerializer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;

public class BabyNameColumnMapSerializer extends XmlStringColumnMapSerializer {

    @Override
    protected Map<String, Object> transformColumnMap(Map<String, Object> columnMap) {
        Map<String, Object> transform = new LinkedHashMap<String, Object>();
        for (Map.Entry<String, Object> entry : columnMap.entrySet()) {
            switch (entry.getKey()) {
                case "BRTH_YR":
                    transform.put("birthYear", entry.getValue());
                    break;
                case "GNDR":
                    transform.put("gender", entry.getValue());
                    break;
                case "ETHCTY":
                    transform.put("ethnicity", entry.getValue());
                    break;
                case "NM":
                    transform.put("name", entry.getValue());
                    break;
                case "CNT":
                    transform.put("count", entry.getValue());
                    break;
                case "RNK":
                    transform.put("rank", entry.getValue());
                    break;
                default:
                    transform.put(entry.getKey(), entry.getValue());
            }
        }

        //Add a create-date-time field
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
        df.setTimeZone(tz);
        String nowAsISO = df.format(new Date());
        transform.put("create_date", nowAsISO);

        return transform;
    }

}
