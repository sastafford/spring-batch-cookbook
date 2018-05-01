package com.marklogic.batch.tika;

import com.marklogic.junit.Fragment;
import com.marklogic.spring.batch.test.AbstractJobRunnerTest;
import org.jdom2.Namespace;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;

@ContextConfiguration(classes = {ImportAndExtractContentJobConfig.class})
@RunWith(SpringJUnit4ClassRunner.class)
public class ImportAndExtractContentJobTest extends AbstractJobRunnerTest {

    private JobParametersBuilder jpb = new JobParametersBuilder();
    private JobExecution jobExecution;

    @Test
    public void importAndExtractWithTika() throws Exception {
        givenAJob();
        whenTheJobIsExecuted();
        documentExistsInMarkLogic();
    }

    public void givenAJob() {
        jpb.addString("input_file_path", "./src/test/resources/doc/LoremIpsum.docx");
        jpb.addString("output_collections", "tika");
        jpb.addString("chunk_size", "500");
        jpb.addString("thread_count", "2");

    }

    public void whenTheJobIsExecuted() throws Exception {
        jobExecution = getJobLauncherTestUtils().launchJob(jpb.toJobParameters());
        assertThat(jobExecution.getStatus(), is(BatchStatus.COMPLETED));
    }

    public void documentExistsInMarkLogic() {
        List<String> uris = getClientTestHelper().getUrisInCollection("tika", 1);
        Fragment f = getClientTestHelper().parseUri(uris.get(0), "tika");

        Namespace namespace = Namespace.getNamespace("html", "http://www.w3.org/1999/xhtml");
        f.setNamespaces(new Namespace[]{namespace});

        f.assertElementExists("//html:head");
        f.assertElementValue("expecting lorem ipsum", "//html:b", "Lorem Ipsum");
    }
}
