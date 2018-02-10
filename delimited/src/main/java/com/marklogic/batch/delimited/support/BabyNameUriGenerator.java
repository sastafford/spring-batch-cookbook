package com.marklogic.batch.delimited.support;

import com.marklogic.spring.batch.item.processor.support.UriGenerator;

import java.util.Map;
import java.util.UUID;


public class BabyNameUriGenerator implements UriGenerator<Map<String, Object>> {

    @Override
    public String generateUri(Map<String, Object> stringObjectMap) {
        String uuid = UUID.randomUUID().toString();
        String uri = "/" + stringObjectMap.get("BRTH_YR").toString() + "/" + uuid;
        return uri;
    }
}
