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

    @Test
    public void ingestDelimitedBabyNamesWithUriTransformTest() throws Exception {
        JobExecution jobExecution = getJobLauncherTestUtils().launchJob(getJobParametersWithUriTransform());
        getClientTestHelper().assertCollectionSize("Expecting 4882 files in baby-name collection", "baby-name", 4882);
        getClientTestHelper().parseUri("2011-ALEC.xml", "baby-name");
    }

    @Test
    public void ingestBabyNamesToJsonTest() throws Exception {
        JobParametersBuilder jpb = new JobParametersBuilder();
        jpb.addString("input_file_path", ".\\src\\test\\resources\\Most_Popular_Baby_Names_NYC.csv");
        jpb.addString("delimited_root_name", "baby-name");
        jpb.addString("document_type", "xml");
        jpb.addString("output_collections", "baby-name");
        jpb.addString("document_type", "json");
        JobExecution jobExecution = getJobLauncherTestUtils().launchJob(jpb.toJobParameters());
        getClientTestHelper().assertCollectionSize("Expecting 13962 files in baby-name collection", "baby-name", 13962);
    }

    @Test
    public void ingestBabyNamesWithUriIdTest() throws Exception {
        JobParametersBuilder jpb = new JobParametersBuilder();
        jpb.addString("input_file_path", ".\\src\\test\\resources\\Most_Popular_Baby_Names_NYC.csv");
        jpb.addString("delimited_root_name", "baby-name");
        jpb.addString("document_type", "xml");
        jpb.addString("output_collections", "baby-name");
        jpb.addString("uri_id", "NM");
        JobExecution jobExecution = getJobLauncherTestUtils().launchJob(jpb.toJobParameters());
        getClientTestHelper().assertCollectionSize("Expecting 2811 files in baby-name collection", "baby-name", 2811);
        getClientTestHelper().parseUri("ALEC", "baby-name");
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

    protected JobParameters getJobParametersWithUriTransform() {
        JobParametersBuilder jpb = new JobParametersBuilder();
        jpb.addString("input_file_path", ".\\src\\test\\resources\\Most_Popular_Baby_Names_NYC.csv");
        jpb.addString("delimited_root_name", "baby-name");
        jpb.addString("document_type", "xml");
        jpb.addString("output_collections", "baby-name");
        jpb.addString("output_transform", "com.marklogic.hector.BabyNameColumnMapSerializer");
        jpb.addString("uri_transform", "com.marklogic.hector.BabyNameUriGenerator");
        return jpb.toJobParameters();
    }

}


