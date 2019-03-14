package sastafford;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@EnableBatchProcessing
public class HelloWorldJobConfiguration {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    // This is the bean label for the name of your Job.  Pass this label into the job_id parameter
    // when using the CommandLineJobRunner
    private final String JOB_NAME = "helloWorldJob";

    /**
     * The JobBuilderFactory and Step parameters are injected via the EnableBatchProcessing annotation.
     *
     * @param jobBuilderFactory injected from the @EnableBatchProcessing annotation
     * @param step              injected from the step method in this class
     * @return Job
     */
    @Bean(name = JOB_NAME)
    public Job job(JobBuilderFactory jobBuilderFactory, Step step) {
        JobExecutionListener listener = new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                logger.info("BEFORE JOB");
                jobExecution.getExecutionContext().putString("random", "yourJob123");
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
                logger.info("AFTER JOB");
            }
        };

        return jobBuilderFactory.get(JOB_NAME)
                .start(step)
                .listener(listener)
                .incrementer(new RunIdIncrementer())
                .build();
    }

    @Bean
    @JobScope
    public Step step(
            StepBuilderFactory stepBuilderFactory,
            @Value("#{jobParameters['helloworld.name'] ?: 'Joe Cool'}") String name) {

        ItemReader<String> itemReader = new ItemReader<String>() {
            int i = 0;

            @Override
            public String read() throws Exception {
                i++;
                return i <= 10 ? name : null;
            }
        };

        ItemProcessor<String, String> itemProcessor = new ItemProcessor<String, String>() {

            @Override
            public String process(String item) throws Exception {
                return item.toUpperCase();
            }
        };

        ItemWriter<String> itemWriter = new ItemWriter<String>() {

            @Override
            public void write(List<? extends String> items) throws Exception {
                for (String item : items) {
                    logger.info(item);
                }
            }
        };

        return stepBuilderFactory.get("step1")
                .<String, String>chunk(2)
                .reader(itemReader)
                .processor(itemProcessor)
                .writer(itemWriter)
                .build();
    }

}
