package com.marklogic.batch.rdbms;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.ext.helper.DatabaseClientProvider;
import com.marklogic.spring.batch.columnmap.ColumnMapSerializer;
import com.marklogic.spring.batch.columnmap.XmlStringColumnMapSerializer;
import com.marklogic.spring.batch.item.processor.support.UriGenerator;
import com.marklogic.spring.batch.item.writer.MarkLogicItemWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.Map;
import java.util.UUID;

@EnableBatchProcessing(modular = true)
@Import(value = {
    com.marklogic.spring.batch.config.MarkLogicConfiguration.class,
    IngestCustomersToMarkLogicJobConfig.BasicBatchConfigurer.class})
@PropertySource("classpath:job.properties")
public class IngestCustomersToMarkLogicJobConfig {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private final String JOB_NAME = "ingestCustomersToMarkLogicJob";


    @Bean(name = JOB_NAME)
    public Job job(JobBuilderFactory jobBuilderFactory, Step step) {
        JobExecutionListener listener = new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                logger.info("BEFORE JOB");
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
                logger.info("AFTER JOB");
            }
        };

        return jobBuilderFactory.get(JOB_NAME)
            .start(step)
            .incrementer(new RunIdIncrementer())
            .listener(listener)
            .build();
    }

    @Bean
    @JobScope
    public Step step(
        StepBuilderFactory stepBuilderFactory,
        @Qualifier("customerDatabase") DataSource dataSource,
        DatabaseClientProvider databaseClientProvider) {

        JdbcCursorItemReader<Map<String, Object>> reader = new JdbcCursorItemReader<Map<String, Object>>();
        reader.setDataSource(dataSource);
        reader.setSql("SELECT * FROM CUSTOMER");
        reader.setRowMapper(new ColumnMapRowMapper());

        // Processor - this is a very basic implementation for converting a column map to an XML string
        ColumnMapSerializer serializer = new XmlStringColumnMapSerializer();
        UriGenerator<Map<String, Object>> uriGenerator = new UriGenerator<Map<String, Object>>() {
            @Override
            public String generateUri(Map<String, Object> s) throws Exception {
                return UUID.randomUUID().toString() + ".xml";
            }
        };
        ColumnMapItemProcessor processor = new ColumnMapItemProcessor(uriGenerator, serializer);
        processor.setCollections(new String[]{"CUSTOMER"});

        ItemWriter writer = new MarkLogicItemWriter(databaseClientProvider.getDatabaseClient());

        return stepBuilderFactory.get("step1")
            .<Map<String, Object>, DocumentWriteOperation>chunk(10)
            .reader(reader)
            .processor(processor)
            .writer(writer)
            .build();
    }

    @Bean
    @Qualifier("customerDatabase")
    public DataSource dataSource(
        @Value("${jdbc.driver:null}") String jdbcDriverClassName,
        @Value("${jdbc.url:null}") String jdbcUrl,
        @Value("${jdbc.username:sa}") String username,
        @Value("${jdbc.password:sa}") String password) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(jdbcDriverClassName);
        dataSource.setUrl(jdbcUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        return dataSource;
    }

    @Component
    public class BasicBatchConfigurer extends DefaultBatchConfigurer {

        @Override
        public void setDataSource(DataSource dataSource) {

        }
    }
}
