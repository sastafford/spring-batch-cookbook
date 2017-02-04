package com.marklogic.hector;

import com.marklogic.spring.batch.test.AbstractJobRunnerTest;
import org.junit.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(classes = { ImportDelimitedFileJob.class })
public class ImportDelimitedFileJobTest extends AbstractJobRunnerTest {

    @Test
    public void ingestDelimitedBabyNamesTest() throws Exception {
        JobParametersBuilder jpb = new JobParametersBuilder();
        jpb.addString("input_file_path", ".\\src\\test\\resources\\Most_Popular_Baby_Names_NYC.csv");
        jpb.addString("delimited_root_name", "baby-name");
        jpb.addString("document_type", "xml");
        jpb.addString("output_collections", "baby-name");
        jpb.addString("output_transform", "com.marklogic.hector.BabyNameColumnMapSerializer");
        JobExecution jobExecution = getJobLauncherTestUtils().launchJob(jpb.toJobParameters());

        getClientTestHelper().assertCollectionSize("Expecting 13962 files in baby-name collection", "baby-name", 13962);

    }


}


