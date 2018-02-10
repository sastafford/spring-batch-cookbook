# Learn how to use Spring Batch 

The Spring Batch Cookbook project is a collection of batch processing programs intended to demonstrate how Spring Batch could be used specifically with [MarkLogic](www.marklogic.com).  Use these programs as-is or modify to accomplish your requirements.  

# Prerequisites

 * JDK 1.8
 * MarkLogic 8+
 
# Quick Start

Before running a program, review the job.properties file in the project root directory to make sure that the properties are correct for your environment.   

    marklogic.host=localhost
    marklogic.port=8000
    marklogic.username=admin
    marklogic.password=admin

## delimited 

Import a delimited file containing the most popular baby names in New York City into MarkLogic.  This job demonstrates adding a year prefix to the URI and adding a createDateTime to the document.  

     gradlew importBabyNames

## tika 

The [Apache Tika](https://tika.apache.org/) toolkit detects and extracts metadata and text from over a thousand different file types.  This job uses Apache Tika to extract metadata content and ingest into MarkLogic.

     gradlew import

## rdbms 
import customer data from an H2 relational database

