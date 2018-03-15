package nl.marklogic.sb.helper;

import com.google.gson.JsonElement;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.spring.batch.item.processor.MarkLogicItemProcessor;
import com.marklogic.spring.batch.item.processor.support.UriGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

public class JsonElementItemProcessor implements MarkLogicItemProcessor<JsonElement> {
    private String[] permissions;
    private String[] collections;
    private String type = "document";
    private Format format;
    protected UriGenerator uriGenerator;

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    public JsonElementItemProcessor() {
        uriGenerator = new UriGenerator() {
            @Override
            public String generateUri(Object o) {
                return UUID.randomUUID().toString();
            }
        };
    }

    public JsonElementItemProcessor(UriGenerator uriGenerator) {
        this.uriGenerator = uriGenerator;
    }

    public DocumentWriteOperation process(JsonElement item) throws Exception {
        return new DocumentWriteOperationImpl(
                DocumentWriteOperation.OperationType.DOCUMENT_WRITE,
                uriGenerator.generateUri(item),
                getDocumentMetadata(item),
                getContentHandle(item));
    }

    public StringHandle getContentHandle(JsonElement item) throws Exception {
        return new StringHandle(item.getAsString());
    }

    protected DocumentMetadataHandle getDocumentMetadata(JsonElement item) {
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        if (collections != null) {
            metadata.withCollections(collections);
        }
        if (permissions != null) {
            for (int i = 0; i < permissions.length; i += 2) {
                String role = permissions[i];
                DocumentMetadataHandle.Capability c = DocumentMetadataHandle.Capability.valueOf(permissions[i + 1].toUpperCase());
                metadata.withPermission(role, c);
            }
        }
        return metadata;
    }

    public String transformDateTime(String dateTime, String dateTimeMask) {
        String ISO_DATE_TIME_PATTERN = "yyyy-MM-dd'T'HH:mm'Z'";
        SimpleDateFormat iso8601Formatter = new SimpleDateFormat(ISO_DATE_TIME_PATTERN);
        SimpleDateFormat simpleDateTimeFormatter = new SimpleDateFormat(dateTimeMask);
        Date date = simpleDateTimeFormatter.parse(dateTime, new ParsePosition(0));
        return iso8601Formatter.format(date);
    }

    public String transformDate(String dateTime, String dateMask) {
        String ISO_DATE_PATTERN = "yyyy-MM-dd";
        SimpleDateFormat iso8601Formatter = new SimpleDateFormat(ISO_DATE_PATTERN);
        SimpleDateFormat simpleDateFormatter = new SimpleDateFormat(dateMask);
        Date date = simpleDateFormatter.parse(dateTime, new ParsePosition(0));
        return iso8601Formatter.format(date);
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

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Format getFormat() {
        return format;
    }

    public void setFormat(Format format) {
        this.format = format;
    }

    public UriGenerator getUriGenerator() {
        return uriGenerator;
    }

    public void setUriGenerator(UriGenerator uriGenerator) {
        this.uriGenerator = uriGenerator;
    }

}
