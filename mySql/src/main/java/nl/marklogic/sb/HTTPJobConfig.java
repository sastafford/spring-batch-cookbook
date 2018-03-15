package nl.marklogic.sb;

import com.google.gson.JsonElement;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.ext.helper.DatabaseClientProvider;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.client.io.marker.DocumentMetadataWriteHandle;
import com.marklogic.spring.batch.item.processor.MarkLogicItemProcessor;
import com.marklogic.spring.batch.item.writer.MarkLogicItemWriter;
import com.marklogic.spring.batch.item.writer.support.DefaultUriTransformer;
import nl.marklogic.sb.helper.JsonElementItemProcessor;
import nl.marklogic.sb.helper.JsonMimeInterceptor;
import nl.marklogic.sb.http.HttpFlightDataJsonItemReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.support.BasicAuthorizationInterceptor;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

@EnableBatchProcessing
@Import(value = {
        com.marklogic.spring.batch.config.MarkLogicBatchConfiguration.class,
        com.marklogic.spring.batch.config.MarkLogicConfiguration.class})
@PropertySource("classpath:job.properties")
public class HTTPJobConfig {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    // This is the bean label for the name of your Job.  Pass this label into the job_id parameter
    // when using the CommandLineJobRunner
    private final String JOB_NAME = "httpJob";

    /**
     * Defines the job for Spring Batch to run. This job consists of a single step, defined below.
     */
    @Bean(name=JOB_NAME)
    public Job job(JobBuilderFactory jobBuilderFactory, Step step) {
        return jobBuilderFactory.get(JOB_NAME)
                .start(step)
                .build();
    }
    /**
     * The StepBuilderFactory and DatabaseClientProvider parameters are injected via Spring.  Custom parameters must be annotated with @Value.
     *
     * @param stepBuilderFactory     injected from the @EnableBatchProcessing annotation
     * @param databaseClientProvider injected from the BasicConfig class
     * @param collections            This is an example of how user parameters could be injected via command line or a properties file
     * @param chunkSize              This is an example of how user parameters could be injected via command line or a properties file
     * @return Step
     * @see DatabaseClientProvider
     * @see ItemReader
     * @see DocumentWriteOperation
     * @see MarkLogicItemProcessor
     * @see MarkLogicItemWriter
     */
    @Bean
    @JobScope
    public Step step(StepBuilderFactory stepBuilderFactory,
                     DatabaseClientProvider databaseClientProvider,
                     @Value("#{jobParameters['http_url']}") String httpUrl,
                     @Value("#{jobParameters['http_username']}") String httpUsername,
                     @Value("#{jobParameters['http_password']}") String httpPassword,
                     @Value("#{jobParameters['chunk_size']}") Integer chunkSize,
                     @Value("#{jobParameters['collections']}") String collections,
                     @Value("#{jobParameters['document_type']}") String documentType,
                     @Value("#{jobParameters['output_uri_prefix']}") String outputUriPrefix,
                     @Value("#{jobParameters['permissions']}") String permissions,
                     @Value("#{jobParameters['query']}") String query,
                     @Value("#{jobParameters['thread_count']}") Integer threadCount) throws URISyntaxException {

        String[] colls = collections.split(",");
        String[] perms = permissions.split(",");

        RestTemplate restTemplate = new RestTemplate();
        List<ClientHttpRequestInterceptor> intercepts = new ArrayList<ClientHttpRequestInterceptor>();
        intercepts.add(new BasicAuthorizationInterceptor(httpUsername, httpPassword));
        intercepts.add(new JsonMimeInterceptor());
        restTemplate.setInterceptors(intercepts);

        ItemStreamReader<String> reader = null;
        reader = new HttpFlightDataJsonItemReader(restTemplate,new URI(httpUrl), query, "hits");

        //The ItemProcessor is typically customized for your Job.  An anoymous class is a nice way to instantiate but
        //if it is a reusable component instantiate in its own class
        MarkLogicItemProcessor<String> processor = new MarkLogicItemProcessor<String>() {

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
                        if (collections != null) {
                            metadata.withCollections(collections.split(","));
                        }
                        if (permissions != null) {
                            String[] perms = permissions.split(",");
                            for (int i = 0; i < perms.length; i += 2) {
                                String role = perms[i];
                                DocumentMetadataHandle.Capability c = DocumentMetadataHandle.Capability.valueOf(perms[i + 1].toUpperCase());
                                metadata.withPermission(role, c);
                            }
                        }
                        return metadata;
                    }

                    @Override
                    public AbstractWriteHandle getContent() {
                        return new StringHandle(item);
                    }

                    @Override
                    public String getTemporalDocumentURI() {
                        return null;
                    }
                };
                return dwo;
            }
         };

        // marklogic-spring-batch component for generating a URI for a document
        DefaultUriTransformer uriTransformer = new DefaultUriTransformer();
        uriTransformer.setOutputUriPrefix(outputUriPrefix);
        DatabaseClient databaseClient = databaseClientProvider.getDatabaseClient();
        MarkLogicItemWriter writer = new MarkLogicItemWriter(databaseClient);
        writer.setBatchSize(chunkSize);
        writer.setUriTransformer(uriTransformer);
        writer.setThreadCount(threadCount);
        writer.setBatchSize(chunkSize);

        ChunkListener chunkListener = new ChunkListener() {

            @Override
            public void beforeChunk(ChunkContext context) {
                logger.info("beforeChunk");
            }

            @Override
            public void afterChunk(ChunkContext context) {
                logger.info("afterChunk");
            }

            @Override
            public void afterChunkError(ChunkContext context) {

            }
        };

        // Return a step with the reader, processor, and writer constructed above.
        return stepBuilderFactory.get("httpJobStep")
                .<String, DocumentWriteOperation>chunk(chunkSize)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .listener(chunkListener)
                .build();

    }
}
