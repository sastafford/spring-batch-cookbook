package com.marklogic.batch.item.reader;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.QueryBatch;
import com.marklogic.client.datamovement.QueryBatchListener;
import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.document.DocumentPage;
import com.marklogic.client.document.DocumentRecord;
import com.marklogic.client.document.GenericDocumentManager;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.query.StructuredQueryDefinition;
import org.springframework.batch.item.*;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MarkLogicItemReader implements QueryBatchListener, ItemReader<DocumentRecord>, ItemStream {

    protected DatabaseClient databaseClient;
    protected QueryBatcher batcher;
    protected StructuredQueryDefinition query;
    protected Queue<DocumentRecord> documentQueue;
    protected GenericDocumentManager docMgr;
    protected DataMovementManager dataMovementManger;
    protected ServerTransform serverTransform;

    public MarkLogicItemReader(DatabaseClient databaseClient, StructuredQueryDefinition sqd, ServerTransform serverTransform) {
        this.databaseClient = databaseClient;
        this.query = query;
        this.serverTransform = serverTransform;
        documentQueue = new ConcurrentLinkedQueue<DocumentRecord>();
        docMgr = databaseClient.newDocumentManager();
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        dataMovementManger = databaseClient.newDataMovementManager();
        batcher = dataMovementManger.newQueryBatcher(query)
                    .withConsistentSnapshot()
                    .withBatchSize(10)
                    .withThreadCount(1)
                    .onUrisReady(this);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
    }

    @Override
    public void close() throws ItemStreamException {
        batcher.awaitCompletion();
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
