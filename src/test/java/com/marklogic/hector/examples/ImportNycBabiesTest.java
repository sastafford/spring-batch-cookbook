package com.marklogic.hector.examples;

import com.marklogic.hector.ImportDelimitedFileJob;
import com.marklogic.junit.Fragment;
import com.marklogic.spring.batch.test.AbstractJobRunnerTest;
import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration(classes = { ImportDelimitedFileJob.class })
public class ImportNycBabiesTest extends AbstractJobRunnerTest {

    private JobParametersBuilder jpb = new JobParametersBuilder();

    @Before
    public void initJobParameters() {
        jpb.addString("input_file_path", ".\\src\\test\\resources\\baby-names.csv");
        jpb.addString("delimited_root_name", "baby-name");
        jpb.addString("output_collections", "baby-name");

    }

    @Test
    public void ingestDelimitedBabyNamesWithOutputTransformTest() throws Exception {
        jpb.addString("uri_transform", "com.marklogic.hector.examples.babies.BabyNameUriGenerator");
        jpb.addString("output_transform", "com.marklogic.hector.examples.babies.BabyNameColumnMapSerializer");
        JobExecution jobExecution = getJobLauncherTestUtils().launchJob(jpb.toJobParameters());
        getClientTestHelper().assertCollectionSize("Expecting 199 files in baby-name collection", "baby-name", 199);
        Fragment f = getClientTestHelper().parseUri("2011-HAZEL.xml", "baby-name");
        f.assertElementExists("Expecting birth year", "/baby-name/birthYear");
        f.assertElementExists("Expecting birth year", "/baby-name/create_date");
    }

    @Test
    public void ingestDelimitedBabyNamesDefaultThreadAndChunkSizeTest() throws Exception {
        JobExecution jobExecution = getJobLauncherTestUtils().launchJob(jpb.toJobParameters());
        getClientTestHelper().assertCollectionSize("Expecting 199 files in baby-name collection", "baby-name", 199);
    }

    @Test
    public void ingestDelimitedBabyNamesWithUriTransformTest() throws Exception {
        jpb.addString("uri_transform", "com.marklogic.hector.examples.babies.BabyNameUriGenerator");
        JobExecution jobExecution = getJobLauncherTestUtils().launchJob(jpb.toJobParameters());
        getClientTestHelper().assertCollectionSize("Expecting 199 files in baby-name collection", "baby-name", 199);
        getClientTestHelper().parseUri("2011-HAZEL.xml", "baby-name");
    }

    @Test
    public void ingestBabyNamesToJsonTest() throws Exception {
        jpb.addString("document_type", "json");
        JobExecution jobExecution = getJobLauncherTestUtils().launchJob(jpb.toJobParameters());
        getClientTestHelper().assertCollectionSize("Expecting 199 files in baby-name collection", "baby-name", 199);

    }

    @Test
    public void ingestBabyNamesWithDefaultDelimitedRootNameTest() throws Exception {
        JobParametersBuilder jpb = new JobParametersBuilder();
        jpb.addString("input_file_path", ".\\src\\test\\resources\\baby-names.csv");
        jpb.addString("output_collections", "baby-name");
        jpb.addString("uri_transform", "com.marklogic.hector.examples.babies.BabyNameUriGenerator");
        JobExecution jobExecution = getJobLauncherTestUtils().launchJob(jpb.toJobParameters());
        getClientTestHelper().assertCollectionSize("Expecting 199 files in baby-name collection", "baby-name", 199);
        Fragment f = getClientTestHelper().parseUri("2011-HAZEL.xml", "baby-name");
        f.assertElementExists("Expecting default delimited root name", "/record");


    }

    @Test
    public void ingestBabyNamesWithUriIdTest() throws Exception {
        jpb.addString("uri_id", "NM");
        JobExecution jobExecution = getJobLauncherTestUtils().launchJob(jpb.toJobParameters());
        getClientTestHelper().assertCollectionSize("Expecting 2811 files in baby-name collection", "baby-name", 199);
        getClientTestHelper().parseUri("HAZEL", "baby-name");
    }

}


