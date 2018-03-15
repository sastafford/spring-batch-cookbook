package nl.marklogic.sb.http;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.*;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.lang.reflect.Type;
import java.net.URI;
import java.util.Iterator;
import java.util.List;

public class HttpFlightDataJsonItemReader implements ItemStreamReader<String> {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private URI uri;
    private String query;
    private String aggregateRecordElement;
    private RestTemplate restTemplate;
    private Iterator<JsonElement> aggregateRecordElementItr;
    private int count = 0;

    public HttpFlightDataJsonItemReader(RestTemplate restTemplate, URI uri, String  query, String aggregateRecordElement) {
        this.restTemplate = restTemplate;
        this.uri = uri;
        this.query = query;
        this.aggregateRecordElement = aggregateRecordElement;
    }

    @Override
    public String read() throws UnexpectedInputException, ParseException, NonTransientResourceException {
        String doc = null;
        if (aggregateRecordElementItr.hasNext()) {
            JsonElement item = aggregateRecordElementItr.next();
            logger.info("Item " + count + " Id " + item.getAsJsonObject().get("_id").getAsString());
            count++;
            doc = item.toString();
        }
        return doc;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        HttpEntity<String> request = new HttpEntity<String>(query);

        ResponseEntity<String> response = restTemplate.exchange(uri, HttpMethod.POST, request, String.class);
        if (!response.getStatusCode().equals(HttpStatus.OK)) {
            throw new ItemStreamException(response.getStatusCode() + " HTTP response is returned");
        }
        MediaType type = response.getHeaders().getContentType();
        if (MediaType.APPLICATION_JSON.equals(type) ||
                MediaType.APPLICATION_JSON_UTF8.equals(type) ||
                "application/x-ndjson".equals(type.toString())) {
            JsonParser parser = new JsonParser();
            Gson gson = new Gson();
            try {
                JsonElement hits = parser.parse(response.getBody()).getAsJsonObject().get("responses").getAsJsonArray().get(0).getAsJsonObject().getAsJsonObject("hits");
                JsonArray hitsArray = hits.getAsJsonObject().getAsJsonArray(aggregateRecordElement);
                Type listType = new TypeToken<List<JsonElement>>(){}.getType();
                List<JsonElement> hitsList = gson.fromJson(hitsArray.toString(), listType);

                aggregateRecordElementItr = hitsList.listIterator();
            } catch (Exception ex) {
                throw new ItemStreamException(ex);
            }
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        executionContext.putInt("count", count);
    }

    @Override
    public void close() throws ItemStreamException {

    }
}
