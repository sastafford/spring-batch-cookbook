package com.marklogic.hector;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.helper.DatabaseClientProvider;
import com.marklogic.client.helper.LoggingObject;
import com.marklogic.client.io.Format;
import com.marklogic.spring.batch.columnmap.ColumnMapSerializer;
import com.marklogic.spring.batch.columnmap.JacksonColumnMapSerializer;
import com.marklogic.spring.batch.item.processor.ColumnMapProcessor;
import com.marklogic.spring.batch.item.processor.support.UriGenerator;
import com.marklogic.spring.batch.item.writer.MarkLogicItemWriter;
import com.marklogic.spring.batch.item.writer.support.TempRestBatchWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineCallbackHandler;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.FileSystemResource;
import org.springframework.validation.BindException;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.lang.ClassLoader.getSystemClassLoader;

@EnableBatchProcessing
public class ImportDelimitedFileJob extends LoggingObject {

    @Autowired
    DatabaseClientProvider databaseClientProvider;

    @Bean
    @Primary
    public Job job(JobBuilderFactory jobBuilderFactory,
                   Step importDelimitedFileStep) {
        return jobBuilderFactory.get("importDelimitedFile").start(importDelimitedFileStep).build();
    }

    @Bean
    @JobScope
    public Step importDelimitedFileStep(
            StepBuilderFactory stepBuilderFactory,
            DatabaseClientProvider databaseClientProvider,
            @Value("#{jobParameters['input_file_path']}") String inputFilePath,
            @Value("#{jobParameters['document_type'] ?: \"xml\"}") String documentType,
            @Value("#{jobParameters['delimited_root_name']}") String delimitedRootName,
            @Value("#{jobParameters['item_processor']}") String itemProcessor,
            @Value("#{jobParameters['uri_id']}") String uriId,
            @Value("#{jobParameters['uri_transform']}") String uriTransform,
            @Value("#{jobParameters['output_collections']}") String[] collections,
            @Value("#{jobParameters['output_transform']}") String outputTransform,
            @Value("#{jobParameters['thread_count'] ?: 4L}") Long threadCount,
            @Value("#{jobParameters['chunk_size'] ?: 5L}") Long chunkSize) throws Exception {

        FlatFileItemReader<Map<String, Object>> itemReader = new FlatFileItemReader<Map<String, Object>>();
        itemReader.setResource(new FileSystemResource(inputFilePath));
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer(DelimitedLineTokenizer.DELIMITER_COMMA);
        FieldSetMapper<Map<String, Object>> fieldSetMapper = new FieldSetMapper<Map<String, Object>>() {

            @Override
            public Map<String, Object> mapFieldSet(FieldSet fieldSet) throws BindException {
                Map<String, Object> results = new LinkedHashMap<String, Object>();
                String[] names = fieldSet.getNames();
                String[] values = fieldSet.getValues();

                for (int i = 0; i < fieldSet.getFieldCount(); i++) {
                    results.put(names[i], values[i]);
                }
                return results;
            }
        };

        //The following lines will read in the first line has the headers row.
        //Notice that the headers are registered with the ItemProcessor.  Very important
        itemReader.setSkippedLinesCallback(new LineCallbackHandler() {

            @Override
            public void handleLine(String line) {
                FieldSet fs = tokenizer.tokenize(line);
                tokenizer.setNames(fs.getValues());
            }
        });
        itemReader.setLinesToSkip(1);

        DefaultLineMapper<Map<String, Object>> lineMapper = new DefaultLineMapper<Map<String, Object>>();
        lineMapper.setFieldSetMapper(fieldSetMapper);
        lineMapper.setLineTokenizer(tokenizer);
        itemReader.setLineMapper(lineMapper);

        UriGenerator uriGenerator = null;
        if (uriTransform == null) {
            uriGenerator = new DelimitedFileUriGenerator(uriId);
        } else {
            uriGenerator = (UriGenerator) getSystemClassLoader().loadClass(uriTransform).newInstance();
        }

        ColumnMapProcessor processor = null;
        if (outputTransform == null) {
            if ("XML".equals(documentType.toUpperCase())) {
                processor = new ColumnMapProcessor(new XmlStringColumnMapSerializer(), uriGenerator);
            } else if ("JSON".equals(documentType.toUpperCase())) {
                processor = new ColumnMapProcessor(new JacksonColumnMapSerializer(), uriGenerator);
            }
        } else {
            ColumnMapSerializer serializer = (ColumnMapSerializer) getSystemClassLoader().loadClass(outputTransform).newInstance();
            processor = new ColumnMapProcessor(serializer, uriGenerator);
        }

        processor.setCollections(collections);
        processor.setRootLocalName(delimitedRootName);

        TempRestBatchWriter batchWriter = new TempRestBatchWriter(databaseClientProvider.getDatabaseClient());
        batchWriter.setReturnFormat(Format.valueOf(documentType.toUpperCase()));
        batchWriter.setThreadCount(threadCount.intValue());
        MarkLogicItemWriter itemWriter = new MarkLogicItemWriter(batchWriter);


        return stepBuilderFactory.get("step")
                .<Map<String, Object>, DocumentWriteOperation>chunk(chunkSize.intValue())
                .reader(itemReader)
                .processor(processor)
                .writer(itemWriter)
                .build();
    }

}
