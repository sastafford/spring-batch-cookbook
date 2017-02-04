package com.marklogic.hector;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.helper.DatabaseClientProvider;
import com.marklogic.client.helper.LoggingObject;
import com.marklogic.client.io.Format;
import com.marklogic.spring.batch.columnmap.ColumnMapSerializer;
import com.marklogic.spring.batch.item.processor.ColumnMapProcessor;
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

    private String[] headers;

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
            @Value("#{jobParameters['document_type']}") String documentType,
            @Value("#{jobParameters['delimited_root_name']}") String delimitedRootName,
            @Value("#{jobParameters['uri_id']}") String uriId,
            @Value("#{jobParameters['output_collections']}") String[] collections,
            @Value("#{jobParameters['output_transform']}") String outputTransform) throws Exception {

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
                //itemProcessor.setHeaders(fs.getValues());
            }
        });
        itemReader.setLinesToSkip(1);

        DefaultLineMapper<Map<String, Object>> lineMapper = new DefaultLineMapper<Map<String, Object>>();
        lineMapper.setFieldSetMapper(fieldSetMapper);
        lineMapper.setLineTokenizer(tokenizer);
        itemReader.setLineMapper(lineMapper);

        ColumnMapProcessor itemProcessor = null;
        if (outputTransform == null) {
            itemProcessor = new ColumnMapProcessor(new XmlStringColumnMapSerializer());
        } else {
            ColumnMapSerializer serializer = (ColumnMapSerializer) getSystemClassLoader().loadClass(outputTransform).newInstance();
            itemProcessor = new ColumnMapProcessor(serializer);
        }

        itemProcessor.setCollections(collections);
        itemProcessor.setRootLocalName(delimitedRootName);

        TempRestBatchWriter batchWriter = new TempRestBatchWriter(databaseClientProvider.getDatabaseClient());
        batchWriter.setReturnFormat(Format.XML);
        batchWriter.setThreadCount(1);
        MarkLogicItemWriter itemWriter = new MarkLogicItemWriter(batchWriter);


        return stepBuilderFactory.get("step")
                .<Map<String, Object>, DocumentWriteOperation>chunk(100)
                .reader(itemReader)
                .processor(itemProcessor)
                .writer(itemWriter)
                .build();
    }

}
