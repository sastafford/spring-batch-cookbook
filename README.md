# Learn how to use Spring Batch 

The Spring Batch Cookbook project is a collection of batch processing programs intended to demonstrate how Spring Batch could be used specifically with [MarkLogic](www.marklogic.com).  Use these programs as-is or modify to accomplish your requirements.  

# Prerequisites

 * [JDK 1.8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
 * [MarkLogic 9+](https://developer.marklogic.com/products)
 
# Quick Start

To begin, clone this git repository.

     https://github.com/sastafford/spring-batch-cookbook.git

Before running a program, review the job.properties file in the project root directory to make sure that the properties are correct for your environment.  By default the appserver on port 8000 points to the "Documents" database.  

    marklogic.host=localhost
    marklogic.port=8000
    marklogic.username=admin
    marklogic.password=admin

## fixed width

Import a [fixed width file](https://github.com/sastafford/spring-batch-cookbook/blob/dev/fixed-width/src/test/resources/popular-baby-names.txt) containing the most popular baby names in New York into MarkLogic.  
 
     gradlew importFixedWidthFile

After job executes, there will be 13962 names in your target MarkLogic database.  

## tika 

The [Apache Tika](https://tika.apache.org/) toolkit detects and extracts metadata and text from over a thousand different file types.  This job uses Apache Tika to extract metadata content and ingest into MarkLogic.

     gradlew importAndExtract

After job executes, there will be 1 document that exists in the target database in the 'tika' collection.  The contents will be in an HTML format with the metadata and content extracted out of a Microsoft Word document (docx).  

## rdbms 

Import customer data from an Invoices relational database.  The relational database used is [H2](http://www.h2database.com/html/main.html).  Before the job is executed, the invoices are loaded into the H2 database. The data from the customers table is loaded into MarkLogic.   

     gradlew ingestCustomers

After the job executes, there should be 50 customer documents in the CUSTOMER collection.  

To view the contents of the invoice relational database, use the following command. 

     gradlew runH2DataManager

## delimited 

Import a delimited file containing the most popular baby names in New York City into MarkLogic.  This job demonstrates adding a year prefix to the URI and adding a createDateTime to the document.  

     gradlew importBabyNames

After job executes, there will be 13962 documents that exist in the target database in the 'baby-name' collection.

The basic functionality of this program can also be executed with MarkLogic Content Pump.  In the case, where you may have multiple rows per record, need to skip lines, or have special formatting considerations, then this implementation should be considered.

