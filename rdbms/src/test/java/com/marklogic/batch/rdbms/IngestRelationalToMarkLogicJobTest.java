package com.marklogic.batch.rdbms;

import com.marklogic.spring.batch.test.AbstractJobRunnerTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Date;

@ContextConfiguration(classes = {IngestRelationalToMarkLogicJobConfig.class, H2DatabaseConfiguration.class})
@RunWith(SpringJUnit4ClassRunner.class)
public class IngestRelationalToMarkLogicJobTest extends AbstractJobRunnerTest {

    @Autowired
    JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    public void migrateCustomersToDataHubTest() {
        JobParametersBuilder jpb = new JobParametersBuilder();
        jpb.addString("allTables", "true");

        try {
            JobExecution execution = jobLauncherTestUtils.launchJob(jpb.toJobParameters());
            assertEquals(BatchStatus.COMPLETED, execution.getStatus());
        } catch (Exception e) {
            e.printStackTrace();
        }

        getClientTestHelper().assertCollectionSize("CUSTOMER = 50", "CUSTOMER", 50);
        //getClientTestHelper().assertCollectionSize("INVOICE = 50", "INVOICE", 50);
        //getClientTestHelper().assertCollectionSize("ITEM = 650", "ITEM", 650);
        //getClientTestHelper().assertCollectionSize("PRODUCT = 50", "PRODUCT", 50);

    }
}
