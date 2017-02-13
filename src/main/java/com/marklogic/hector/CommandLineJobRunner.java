package com.marklogic.hector;

import org.apache.commons.lang3.ArrayUtils;

public class CommandLineJobRunner {

    public static void main(String[] args) throws Exception {
        String[] newArgs = ArrayUtils.addAll(new String[]{"com.marklogic.hector.JobConfig", "hector"}, args);
        org.springframework.batch.core.launch.support.CommandLineJobRunner.main(newArgs);
    }
}
