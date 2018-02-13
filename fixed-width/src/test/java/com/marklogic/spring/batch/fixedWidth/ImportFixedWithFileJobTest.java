package com.marklogic.spring.batch.fixedWidth;

import com.marklogic.spring.batch.test.AbstractJobRunnerTest;
import org.junit.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.test.context.ContextConfiguration;

import static org.hamcrest.CoreMatchers.is;

@ContextConfiguration(classes = {ImportFixedWidthFileJobConfig.class})
public class ImportFixedWithFileJobTest extends AbstractJobRunnerTest {

    private JobParametersBuilder jpb = new JobParametersBuilder();
    private JobExecution jobExecution;

    @Test
    public void ingestFixedWidthFile() throws Exception {
        givenAJob();
        whenTheJobIsExecuted();
        thenBabyNamesExistInMarkLogic();
    }

    public void givenAJob() {
        jpb.addString("input_file_path", "./src/test/resources/popular-baby-names.txt");
    }

    public void whenTheJobIsExecuted() throws Exception {
        jobExecution = getJobLauncherTestUtils().launchJob(jpb.toJobParameters());
        assertThat(jobExecution.getStatus(), is(BatchStatus.COMPLETED));
    }

    public void thenBabyNamesExistInMarkLogic() {
        getClientTestHelper().assertCollectionSize("Expect 13962 documents","baby-name", 13962);
    }
}
