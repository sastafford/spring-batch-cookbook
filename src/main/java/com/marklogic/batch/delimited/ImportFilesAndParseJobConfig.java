package com.marklogic.batch.delimited;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.ext.helper.DatabaseClientProvider;
import com.marklogic.spring.batch.config.MarkLogicBatchConfiguration;
import com.marklogic.spring.batch.config.MarkLogicConfiguration;
import com.marklogic.spring.batch.item.file.EnhancedResourcesItemReader;
import com.marklogic.spring.batch.item.file.TikaParserItemProcessor;
import com.marklogic.spring.batch.item.writer.MarkLogicItemWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.Resource;

@EnableBatchProcessing
@Import( {MarkLogicBatchConfiguration.class, MarkLogicConfiguration.class} )
@PropertySource("classpath:job.properties")
public class ImportFilesAndParseJobConfig {

    private final static Logger logger = LoggerFactory.getLogger(MarkLogicItemWriter.class);

    @Autowired
    DatabaseClientProvider databaseClientProvider;

    @Bean(name = "importFilesAndParseJob")
    @Primary
    public Job loadDocumentsFromDirectoryJob(
            JobBuilderFactory jobBuilderFactory,
            @Qualifier("loadDocumentsFromDirectoryJobStep1") Step step) {
        return jobBuilderFactory.get("loadImagesFromDirectoryJob")
                .incrementer(new RunIdIncrementer())
                .start(step)
                .build();
    }

    @Bean
    @JobScope
    public Step loadDocumentsFromDirectoryJobStep1(
            StepBuilderFactory stepBuilderFactory,
            DatabaseClientProvider databaseClientProvider,
            @Value("#{jobParameters['output_collections']}") String outputCollections,
            @Value("#{jobParameters['input_file_path']}") String inputFilePath,
            @Value("#{jobParameters['input_file_pattern']}") String inputFilePattern,
            @Value("#{jobParameters['thread_count']}") int threadCount,
            @Value("#{jobParameters['chunk_size']}") int chunkSize) {

        EnhancedResourcesItemReader itemReader = new EnhancedResourcesItemReader(inputFilePath, inputFilePattern);

        TikaParserItemProcessor itemProcessor = new TikaParserItemProcessor();
        itemProcessor.setCollections(outputCollections.split(","));

        MarkLogicItemWriter itemWriter = new MarkLogicItemWriter(databaseClientProvider.getDatabaseClient());
        itemWriter.setBatchSize(chunkSize);
        itemWriter.setThreadCount(threadCount);

        return stepBuilderFactory.get("step1")
                .<Resource, DocumentWriteOperation>chunk(10)
                .reader(itemReader)
                .processor(itemProcessor)
                .writer(itemWriter)
                .build();
    }
}
