package com.marklogic.spring.batch.item.reader;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentRecord;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StringQueryDefinition;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.client.query.StructuredQueryDefinition;
import com.marklogic.junit.spring.AbstractSpringTest;
import com.marklogic.spring.batch.config.MarkLogicConfiguration;
import com.marklogic.spring.batch.test.AbstractSpringBatchTest;
import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@ContextConfiguration(classes = MarkLogicConfiguration.class)
@TestPropertySource("classpath:job.properties")
public class MarkLogicItemReaderTest extends AbstractSpringBatchTest {

    private MarkLogicItemReader itemReader;
    private DatabaseClient client;

    @Before
    public void before() {
        client = getClient();
    }

    @Test
    public void readDocumentUsingStructuredQueryDefinitionTest() throws Exception {
        itemReader = new MarkLogicItemReader(getClient(), getStructuredQueryDefinition());
        String uri = "/docs/doc.xml";
        insertDocument(uri);
        itemReader.open(new ExecutionContext());
        DocumentRecord record = itemReader.read();
        assertEquals(record.getUri(), uri);
        itemReader.close();
    }

    @Test
    public void readTwoDocumentsUsingStructuredQueryDefinitionTest() throws Exception {
        itemReader = new MarkLogicItemReader(getClient(), getStructuredQueryDefinition());
        String firstUri = "/docs/a.xml";
        String secondUri = "/docs/b.xml";
        insertDocument(firstUri);
        insertDocument(secondUri);

        itemReader.open(new ExecutionContext());
        DocumentRecord record = itemReader.read();
        assertEquals(record.getUri(), firstUri);
        record = itemReader.read();
        assertEquals(record.getUri(), secondUri);
        itemReader.close();
    }

    @Test
    public void readDocumentUsingStringQueryDefinitionTest() throws Exception {
        itemReader = new MarkLogicItemReader(getClient(), getStringQueryDefinition());
        String uri = "/docs/doc.xml";
        insertDocument(uri);
        itemReader.open(new ExecutionContext());
        DocumentRecord record = itemReader.read();
        assertEquals(record.getUri(), uri);
        itemReader.close();
    }

    private void insertDocument(String uri) {
        StringHandle handle = new StringHandle();
        handle.set("<hello>hello</hello>");
        XMLDocumentManager docMgr = client.newXMLDocumentManager();
        docMgr.write(uri, handle);
    }

    private StructuredQueryDefinition getStructuredQueryDefinition() {
        QueryManager qm = client.newQueryManager();
        StructuredQueryBuilder sqb = qm.newStructuredQueryBuilder();
        StructuredQueryDefinition query = sqb.directory(true, "/docs/");
        return query;
    }

    private StringQueryDefinition getStringQueryDefinition() {
        QueryManager qm = client.newQueryManager();
        StringQueryDefinition sqd = qm.newStringDefinition();
        sqd.setCriteria("hello");
        return sqd;
    }
}
