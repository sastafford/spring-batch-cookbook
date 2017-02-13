# Hector - Ingest Delimited Files into MarkLogic

Hector is a command line program that ingests delimited text files into MarkLogic.  Hector is easily extensible allowing developers to transform delimited records before ingesting data into MarkLogic while still providing high performance.  

Hector is built using the [Spring Batch](http://docs.spring.io/spring-batch/trunk/reference/html/) framework and the [MarkLogic Spring Batch](https://github.com/sastafford/marklogic-spring-batch) extensions.  

## Prerequisites

 * JDK 1.8
 * MarkLogic 8+
 * Gradle - know how it is used
 
## How do I install Hector?

1) Clone or download this project.  

2) Create a MarkLogic app server and database for testing purposes.  Review gradle.properties before deploying

     gradlew mlDeploy

3) Run the gradle tests.  Check the job.properties under src/test/resources to make sure it is configured correctly. All tests should pass.

     gradlew test

4) Create a distribution

    gradlew distZip


