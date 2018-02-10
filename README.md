# Learn how to use Spring Batch 

The Spring Batch Cookbook project is a collection of batch processing programs intended to demonstrate how Spring Batch could be used specifically with [MarkLogic](www.marklogic.com).  Use these programs as-is or modify to accomplish your requirements.  

# Prerequisites

 * JDK 1.8
 * MarkLogic 9+
 
# Quick Start

Before running a program, review the job.properties file in the project root directory to make sure that the properties are correct for your environment.   

    marklogic.host=localhost
    marklogic.port=8000
    marklogic.username=admin
    marklogic.password=admin

## delimited 

Import a delimited file containing the most popular baby names in New York City into MarkLogic.  This job demonstrates adding a year prefix to the URI and adding a createDateTime to the document.  

     gradlew importBabyNames

After job executes, there will be 13962 documents that exist in the target database in the 'baby-name' collection.

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
