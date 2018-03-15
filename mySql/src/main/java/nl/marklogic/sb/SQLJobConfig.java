package nl.marklogic.sb;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.ext.helper.DatabaseClientProvider;
import com.marklogic.spring.batch.columnmap.ColumnMapSerializer;
import com.marklogic.spring.batch.columnmap.JacksonColumnMapSerializer;
import com.marklogic.spring.batch.item.processor.ColumnMapProcessor;
import com.marklogic.spring.batch.item.processor.MarkLogicItemProcessor;
import com.marklogic.spring.batch.item.rdbms.AllTablesItemReader;
import com.marklogic.spring.batch.item.writer.MarkLogicItemWriter;
import com.marklogic.spring.batch.item.writer.support.DefaultUriTransformer;
import nl.marklogic.sb.helper.ColumnMapUriGenerator;
import nl.marklogic.sb.helper.ESStaxColumnMapSerializer;
import nl.marklogic.sb.helper.MySqlUriTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.util.Map;

@EnableBatchProcessing
@Import(value = {
        com.marklogic.spring.batch.config.MarkLogicBatchConfiguration.class,
        com.marklogic.spring.batch.config.MarkLogicConfiguration.class})
@PropertySource("classpath:job.properties")
public class SQLJobConfig {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    // This is the bean label for the name of your Job.  Pass this label into the job_id parameter
    // when using the CommandLineJobRunner
    private final String JOB_NAME = "sqlJob";

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
                     @Value("#{jobParameters['jdbc_driver']}") String jdbcDriver,
                     @Value("#{jobParameters['jdbc_url']}") String jdbcUrl,
                     @Value("#{jobParameters['jdbc_username']}") String jdbcUsername,
                     @Value("#{jobParameters['jdbc_password']}") String jdbcPassword,
                     @Value("#{jobParameters['all_tables']}") String allTables,
                     @Value("#{jobParameters['chunk_size']}") Integer chunkSize,
                     @Value("#{jobParameters['collections']}") String collections,
                     @Value("#{jobParameters['document_type']}") String documentType,
                     @Value("#{jobParameters['output_uri_prefix']}") String outputUriPrefix,
                     @Value("#{jobParameters['permissions']}") String permissions,
                     @Value("#{jobParameters['root_local_name']}") String rootLocalName,
                     @Value("#{jobParameters['sql']}") String sql,
                     @Value("#{jobParameters['thread_count']}") Integer threadCount,
                     @Value("#{jobParameters['uri_column']}") String uriCol) {

        // Construct a simple DataSource that Spring Batch will use to connect to an RDBMS
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(jdbcDriver);
        dataSource.setUrl(jdbcUrl);
        dataSource.setUsername(jdbcUsername);
        dataSource.setPassword(jdbcPassword);

        // Construct a Spring Batch ItemReader that can read rows from an RDBMS
        ItemReader<Map<String, Object>> reader = null;
        if ("true".equals(allTables)) {
            // Use AllTablesReader to process rows from every table
            reader = new AllTablesItemReader(dataSource);
        } else {
            // Uses Spring Batch's JdbcCursorItemReader and Spring JDBC's ColumnMapRowMapper to map each row
            // to a Map<String, Object>. Normally, if you want more control, standard practice is to bind column values to
            // a POJO and perform any validation/transformation/etc you need to on that object.
            JdbcCursorItemReader<Map<String, Object>> r = new JdbcCursorItemReader<Map<String, Object>>();
            r.setRowMapper(new ColumnMapRowMapper());
            r.setDataSource(dataSource);
            r.setSql(sql);
            r.setRowMapper(new ColumnMapRowMapper());
            reader = r;
        }

        // marklogic-spring-batch component that is used to write a Spring ColumnMap to an XML or JSON document
        ColumnMapSerializer serializer = null;
        if (documentType != null && documentType.toLowerCase().equals("json")) {
            serializer = new JacksonColumnMapSerializer();
        } else {
            serializer = new ESStaxColumnMapSerializer();
        }

        // marklogic-spring-batch component for converting a Spring ColumnMap into an XML or JSON document
        // that can be written to MarkLogic
        ColumnMapProcessor processor = new ColumnMapProcessor(serializer, new ColumnMapUriGenerator(uriCol));
        if (rootLocalName != null) {
            processor.setRootLocalName(rootLocalName);
        }
        if (collections != null) {
            processor.setCollections(collections.split(","));
        }
        if (permissions != null) {
            processor.setPermissions(permissions.split(","));
        }

        // marklogic-spring-batch component for generating a URI for a document
        MySqlUriTransformer uriTransformer = new MySqlUriTransformer();
        if (documentType != null && documentType.toLowerCase().equals("json")) {
            uriTransformer.setOutputUriSuffix(".json");
        } else {
            uriTransformer.setOutputUriSuffix(".xml");
        }
        uriTransformer.setOutputUriPrefix(outputUriPrefix);

        // Construct a DatabaseClient to connect to MarkLogic. Additional command line arguments can be added to
        // further customize this.
        DatabaseClient client = databaseClientProvider.getDatabaseClient();

        // Spring Batch ItemWriter for writing documents to MarkLogic
        MarkLogicItemWriter writer = new MarkLogicItemWriter(client);
        writer.setUriTransformer(uriTransformer);
        writer.setThreadCount(threadCount);
        writer.setBatchSize(chunkSize);

        // Return a step with the reader, processor, and writer constructed above.
        return stepBuilderFactory.get("sqlJobStep")
                .<Map<String, Object>, DocumentWriteOperation>chunk(chunkSize)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }}
