package sastafford;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@Import(HelloWorldJobConfiguration.class)
@SpringBootApplication
public class App implements ApplicationRunner {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(App.class);
        app.setBannerMode(Banner.Mode.OFF);
        app.setWebApplicationType(WebApplicationType.NONE);
        app.run(args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        logger.info(args.toString());
        for (String optionName : args.getOptionNames()) {
            logger.info(optionName);

        }
    }
}
