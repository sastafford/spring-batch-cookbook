package nl.marklogic.sb.helper;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.client.io.marker.DocumentMetadataWriteHandle;
import com.marklogic.spring.batch.item.processor.MarkLogicItemProcessor;

import java.util.UUID;

public class MarkLogicStringItemProcessor implements MarkLogicItemProcessor<String> {
    private String[] collections;
    private String[] permissions;

    public MarkLogicStringItemProcessor(String[] collections) {
        this.collections = collections;
    }

    public String[] getPermissions() {
        return permissions;
    }

    public void setPermissions(String[] permissions) {
        this.permissions = permissions;
    }

    public String[] getCollections() {
        return collections;
    }

    public void setCollections(String[] collections) {
        this.collections = collections;
    }

    @Override
    public DocumentWriteOperation process(String item) throws Exception {
        DocumentWriteOperation dwo = new DocumentWriteOperation() {

            @Override
            public OperationType getOperationType() {
                return OperationType.DOCUMENT_WRITE;
            }

            @Override
            public String getUri() {
                return UUID.randomUUID().toString() + ".json";
            }

            @Override
            public DocumentMetadataWriteHandle getMetadata() {
                DocumentMetadataHandle metadata = new DocumentMetadataHandle();
                metadata.withCollections(collections);
                return metadata;
            }

            @Override
            public AbstractWriteHandle getContent() {
                return new StringHandle(String.format("{ \"name\" : \"%s\" }", item));
            }

            @Override
            public String getTemporalDocumentURI() {
                return null;
            }
        };
        return dwo;
    }
}
