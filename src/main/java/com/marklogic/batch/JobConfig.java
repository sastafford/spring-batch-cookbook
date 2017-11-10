package com.marklogic.batch;

import com.marklogic.batch.delimited.ImportDelimitedFileJobConfig;
import com.marklogic.spring.batch.config.MarkLogicBatchConfiguration;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.context.annotation.Import;

@EnableBatchProcessing
@Import({MarkLogicBatchConfiguration.class, ImportDelimitedFileJobConfig.class })
public class JobConfig {
}
