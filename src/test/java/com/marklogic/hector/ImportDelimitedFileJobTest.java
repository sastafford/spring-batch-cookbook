package com.marklogic.hector;

import com.marklogic.spring.batch.test.AbstractJobRunnerTest;
import org.junit.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(classes = { ImportDelimitedFileJob.class })
public class ImportDelimitedFileJobTest extends AbstractJobRunnerTest {

    @Test
    public void ingestDelimitedBabyNamesTest() throws Exception {
        JobExecution jobExecution = getJobLauncherTestUtils().launchJob(getJobParameters(1L, 1L));
        getClientTestHelper().assertCollectionSize("Expecting 13962 files in baby-name collection", "baby-name", 13962);
    }

    @Test
    public void ingestDelimitedBabyNamesSixteenThreadsTest() throws Exception {
        JobExecution jobExecution = getJobLauncherTestUtils().launchJob(getJobParameters(16L, 1L));
        getClientTestHelper().assertCollectionSize("Expecting 13962 files in baby-name collection", "baby-name", 13962);
    }

    @Test
    public void ingestDelimitedBabyNamesTwoChunkTest() throws Exception {
        JobExecution jobExecution = getJobLauncherTestUtils().launchJob(getJobParameters(1L, 2L));
        getClientTestHelper().assertCollectionSize("Expecting 13962 files in baby-name collection", "baby-name", 13962);
    }

    @Test
    public void ingestDelimitedBabyNamesSixteenTest() throws Exception {
        JobExecution jobExecution = getJobLauncherTestUtils().launchJob(getJobParameters(4L, 4L));
        getClientTestHelper().assertCollectionSize("Expecting 13962 files in baby-name collection", "baby-name", 13962);
    }

    @Test
    public void ingestDelimitedBabyNamesDefaultThreadAndChunkSizeTest() throws Exception {
        JobExecution jobExecution = getJobLauncherTestUtils().launchJob(getJobParameters());
        getClientTestHelper().assertCollectionSize("Expecting 13962 files in baby-name collection", "baby-name", 13962);
    }

    protected JobParameters getJobParameters() {
        JobParametersBuilder jpb = new JobParametersBuilder();
        jpb.addString("input_file_path", ".\\src\\test\\resources\\Most_Popular_Baby_Names_NYC.csv");
        jpb.addString("delimited_root_name", "baby-name");
        jpb.addString("document_type", "xml");
        jpb.addString("output_collections", "baby-name");
        jpb.addString("output_transform", "com.marklogic.hector.BabyNameColumnMapSerializer");
        return jpb.toJobParameters();
    }

    protected JobParameters getJobParameters(Long threadCount, Long chunkSize) {
        JobParametersBuilder jpb = new JobParametersBuilder();
        jpb.addString("input_file_path", ".\\src\\test\\resources\\Most_Popular_Baby_Names_NYC.csv");
        jpb.addString("delimited_root_name", "baby-name");
        jpb.addString("document_type", "xml");
        jpb.addString("output_collections", "baby-name");
        jpb.addString("output_transform", "com.marklogic.hector.BabyNameColumnMapSerializer");
        jpb.addLong("thread_count", threadCount);
        jpb.addLong("chunk_size", chunkSize);
        return jpb.toJobParameters();
    }

}


