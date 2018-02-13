package com.marklogic.spring.batch.fixedWidth;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.ext.helper.DatabaseClientProvider;
import com.marklogic.spring.batch.columnmap.XmlStringColumnMapSerializer;
import com.marklogic.spring.batch.item.processor.ColumnMapProcessor;
import com.marklogic.spring.batch.item.processor.support.UriGenerator;
import com.marklogic.spring.batch.item.writer.MarkLogicItemWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.validation.BindException;

import java.util.*;

import static java.lang.ClassLoader.getSystemClassLoader;

@EnableBatchProcessing
@Import(value = {com.marklogic.spring.batch.config.MarkLogicBatchConfiguration.class})
@PropertySource("classpath:job.properties")
public class ImportFixedWidthFileJobConfig {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    // This is the bean label for the name of your Job.  Pass this label into the job_id parameter
    // when using the CommandLineJobRunner
    private final String JOB_NAME = "importFixedWidthFile";

    /**
     * The JobBuilderFactory and Step parameters are injected via the EnableBatchProcessing annotation.
     *
     * @param jobBuilderFactory injected from the @EnableBatchProcessing annotation
     * @param step              injected from the step method in this class
     * @return Job
     */
    @Bean(name = JOB_NAME)
    public Job job(JobBuilderFactory jobBuilderFactory, Step step) {
        JobExecutionListener listener = new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                logger.info("BEFORE JOB");
                jobExecution.getExecutionContext().putString("random", "yourJob123");
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
                logger.info("AFTER JOB");
            }
        };

        return jobBuilderFactory.get(JOB_NAME)
                .start(step)
                .listener(listener)
                .incrementer(new RunIdIncrementer())
                .build();
    }


    @Bean
    @JobScope
    public Step importDelimitedFileStep(
            StepBuilderFactory stepBuilderFactory,
            DatabaseClientProvider databaseClientProvider,
            @Value("#{jobParameters['input_file_path']}") String inputFilePath,
            @Value("#{jobParameters['document_type'] ?: \"xml\"}") String documentType,
            @Value("#{jobParameters['delimited_root_name'] ?: \"record\"}") String delimitedRootName,
            @Value("#{jobParameters['uri_id']}") String uriId,
            @Value("#{jobParameters['uri_transform']}") String uriTransform,
            @Value("#{jobParameters['output_collections']}") String collections,
            @Value("#{jobParameters['output_transform']}") String outputTransform,
            @Value("#{jobParameters['thread_count'] ?: 4}") Integer threadCount,
            @Value("#{jobParameters['chunk_size'] ?: 100}") Integer chunkSize) throws Exception {

        DatabaseClient databaseClient = databaseClientProvider.getDatabaseClient();

        logger.info(inputFilePath);

        FlatFileItemReader<Map<String, Object>> reader = new FlatFileItemReader<Map<String, Object>>();
        reader.setResource(new FileSystemResource(inputFilePath));
        DefaultLineMapper<Map<String, Object>> lineMapper = new DefaultLineMapper<Map<String, Object>>();

        FixedLengthTokenizer tokenizer = new FixedLengthTokenizer();
        tokenizer.setColumns(
                new Range(1, 5),
                new Range(6, 12),
                new Range(13, 39),
                new Range(40, 51),
                new Range(52, 55),
                new Range(56, 58));
        lineMapper.setLineTokenizer(tokenizer);

        FieldSetMapper fieldSetMapper = new FieldSetMapper<Map<String, Object>>() {

            @Override
            public Map<String, Object> mapFieldSet(FieldSet fieldSet) throws BindException {
                Map<String, Object> results = new LinkedHashMap<String, Object>();
                String[] names = new String[]{"year", "gender", "ethnicity", "name", "count", "rank"};
                String[] values = fieldSet.getValues();

                for (int i = 0; i < fieldSet.getFieldCount(); i++) {
                    results.put(names[i], values[i].trim());
                }
                return results;
            }
        };
        lineMapper.setFieldSetMapper(fieldSetMapper);

        reader.setLineMapper(lineMapper);

        UriGenerator<Map<String, Object>> uriGenerator = new UriGenerator<Map<String, Object>>() {
            @Override
            public String generateUri(Map<String, Object> stringObjectMap) {
                return UUID.randomUUID().toString() + ".xml";
            }
        };

        ColumnMapProcessor processor = new ColumnMapProcessor(new XmlStringColumnMapSerializer(), uriGenerator);
        processor.setRootLocalName("baby-name");
        processor.setCollections(new String[] {"baby-name"});

        MarkLogicItemWriter writer = new MarkLogicItemWriter(databaseClient);
        writer.setBatchSize(chunkSize);


        return stepBuilderFactory.get("step1")
                .<Map<String, Object>, DocumentWriteOperation>chunk(chunkSize)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

}
