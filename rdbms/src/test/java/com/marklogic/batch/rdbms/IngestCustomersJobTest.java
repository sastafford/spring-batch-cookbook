package com.marklogic.batch.rdbms;

import com.marklogic.spring.batch.test.AbstractJobRunnerTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@ContextConfiguration(classes = {IngestCustomersToMarkLogicJobConfig.class, H2DatabaseConfiguration.class})
@RunWith(SpringJUnit4ClassRunner.class)
public class IngestCustomersJobTest extends AbstractJobRunnerTest {

    JobExecution execution;
    JobParameters jobParameters;

    @Autowired
    Job ingestCustomersToMarkLogicJob;

    @Test
    public void ingestCustomersIntoMarkLogic() {
        givenIngestCustomersJob();
        whenJobExecuties();
        thenCustomersAreInMarkLogic();
    }

    @Before
    public void givenIngestCustomersJob() {
        jobParameters = ingestCustomersToMarkLogicJob.getJobParametersIncrementer().getNext(jobParameters);
    }

    public void whenJobExecuties() {
        try {
            execution = getJobLauncherTestUtils().launchJob(jobParameters);
            assertEquals(BatchStatus.COMPLETED, execution.getStatus());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void thenCustomersAreInMarkLogic() {
        getClientTestHelper().assertCollectionSize("CUSTOMER = 50", "CUSTOMER", 50);
    }
}
