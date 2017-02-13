package com.marklogic.hector.examples;

import com.marklogic.hector.ImportDelimitedFileJob;
import com.marklogic.spring.batch.test.AbstractJobRunnerTest;
import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(classes = { ImportDelimitedFileJob.class })
public class ImportNycDeathsTest extends AbstractJobRunnerTest {

    private JobParametersBuilder jpb = new JobParametersBuilder();

    @Before
    public void initJobParameters() {
        jpb.addString("input_file_path", ".\\src\\test\\resources\\nyc-deaths.csv");
        jpb.addString("output_collections", "nyc");
    }

    @Test
    public void ingestDelimitedBabyNamesDefaultThreadAndChunkSizeTest() throws Exception {
        JobExecution jobExecution = getJobLauncherTestUtils().launchJob(jpb.toJobParameters());
        getClientTestHelper().assertCollectionSize("Expecting 7 files in nyc collection", "nyc", 7);
    }
}
