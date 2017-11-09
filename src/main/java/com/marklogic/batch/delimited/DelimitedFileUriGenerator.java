package com.marklogic.batch.delimited;

import com.marklogic.spring.batch.item.processor.support.UriGenerator;

import java.util.Map;
import java.util.UUID;

public class DelimitedFileUriGenerator implements UriGenerator<Map<String, Object>> {

    private String uriColumnName;

    public DelimitedFileUriGenerator(String columnName) {
        this.uriColumnName = columnName;
    }

    @Override
    public String generateUri(Map<String, Object> stringObjectMap) {
        if (uriColumnName == null) {
            return UUID.randomUUID().toString();
        } else {
            return stringObjectMap.get(uriColumnName).toString();
        }

    }

}
