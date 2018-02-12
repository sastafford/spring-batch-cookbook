package com.marklogic.batch.rdbms;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.spring.batch.columnmap.ColumnMapSerializer;
import com.marklogic.spring.batch.item.processor.AbstractMarkLogicItemProcessor;
import com.marklogic.spring.batch.item.processor.support.UriGenerator;

import java.util.Map;

/**
 * This is a very basic processor for taking a column map (a Map<String, Object>) and serializing it via a
 * ColumnMapSerializer, and then providing very basic support for setting permissions and collections.
 * marklogic-spring-batch provides other options for e.g. customizing the URI. Feel free to customize any way you'd like.
 */
public class ColumnMapItemProcessor extends AbstractMarkLogicItemProcessor<Map<String, Object>> {

    private ColumnMapSerializer columnMapSerializer;
    private String tableNameKey = "_tableName";
    private String rootLocalName = "CHANGEME";


    public ColumnMapItemProcessor(UriGenerator uriGenerator, ColumnMapSerializer columnMapSerializer) {
        super(uriGenerator);
        this.columnMapSerializer = columnMapSerializer;
    }

    @Override
    public AbstractWriteHandle getContentHandle(Map<String, Object> item) throws Exception {
        String content = columnMapSerializer.serializeColumnMap(item, "root");
        return new StringHandle(content);
    }
}
