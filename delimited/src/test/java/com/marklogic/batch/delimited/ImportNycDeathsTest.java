package com.marklogic.batch.delimited;

import com.marklogic.batch.delimited.ImportDelimitedFileJobConfig;
import com.marklogic.spring.batch.test.AbstractJobRunnerTest;
import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(classes = {ImportDelimitedFileJobConfig.class})
public class ImportNycDeathsTest extends AbstractJobRunnerTest {

    private JobParametersBuilder jpb = new JobParametersBuilder();
    private JobExecution jobExecution;

    @Test
    public void ingestDelimitedNycDeathsTest() throws Exception {
        givenJobParameters();
        whenJobIsLaunched();
        getClientTestHelper().assertCollectionSize("Expecting 7 files in nyc collection", "nyc", 7);
    }

    private void givenJobParameters() {
        jpb.addString("input_file_path", "./src/test/resources/delimited/nyc-deaths.csv");
        jpb.addString("output_collections", "nyc");
    }

    private void whenJobIsLaunched() throws Exception {
        jobExecution = getJobLauncherTestUtils().launchJob(jpb.toJobParameters());
    }
}
