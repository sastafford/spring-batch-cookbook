package com.marklogic.spring.batch.item.reader;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.*;
import com.marklogic.client.document.DocumentPage;
import com.marklogic.client.document.DocumentRecord;
import com.marklogic.client.document.GenericDocumentManager;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.StringQueryDefinition;
import com.marklogic.client.query.StructuredQueryDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.*;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MarkLogicItemReader implements QueryBatchListener, ItemStreamReader<DocumentRecord> {

    private final static Logger logger = LoggerFactory.getLogger(MarkLogicItemReader.class);

    protected DatabaseClient databaseClient;
    protected QueryBatcher batcher;
    protected StringQueryDefinition stringQueryDefinition;
    protected StructuredQueryDefinition structuredQueryDefinition;
    protected Queue<DocumentRecord> documentQueue;
    protected GenericDocumentManager docMgr;
    protected DataMovementManager dataMovementManger;
    protected ServerTransform serverTransform;

    protected MarkLogicItemReader(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
        documentQueue = new ConcurrentLinkedQueue<DocumentRecord>();
        docMgr = databaseClient.newDocumentManager();
    }

    public MarkLogicItemReader(DatabaseClient databaseClient, StructuredQueryDefinition structuredQueryDefinition) {
        this(databaseClient);
        this.structuredQueryDefinition = structuredQueryDefinition;
    }

    public MarkLogicItemReader(DatabaseClient databaseClient, StringQueryDefinition stringQueryDefintion) {
        this(databaseClient);
        this.stringQueryDefinition = stringQueryDefintion;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        dataMovementManger = databaseClient.newDataMovementManager();
        if (structuredQueryDefinition != null) {
            batcher = dataMovementManger.newQueryBatcher(structuredQueryDefinition);
        } else if (stringQueryDefinition != null) {
            batcher = dataMovementManger.newQueryBatcher(stringQueryDefinition);
        }
        batcher.withBatchSize(100).withThreadCount(2).onUrisReady(this);
        dataMovementManger.startJob(batcher);
        batcher.awaitCompletion();
        dataMovementManger.stopJob(batcher);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
    }

    @Override
    public void close() throws ItemStreamException {
        dataMovementManger.release();
    }

    @Override
    public DocumentRecord read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        if (documentQueue.isEmpty() && batcher.isStopped()) {
            return null;
        } else if (documentQueue.isEmpty() && batcher.isStarted()) {
            batcher.awaitCompletion();
        }
        return documentQueue.poll();
    }

    @Override
    public void processEvent(QueryBatch batch) {
        DocumentPage page = docMgr.read(serverTransform, batch.getItems());
        while (page.hasNext()) {
            documentQueue.add(page.next());
        }
    }

}
