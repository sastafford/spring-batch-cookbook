package com.marklogic.hector;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({ com.marklogic.spring.batch.config.MarkLogicApplicationContext.class,
        ImportDelimitedFileJob.class})
public class JobConfig {
}
