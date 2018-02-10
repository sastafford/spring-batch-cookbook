# Learn how to use Spring Batch 

The Spring Batch Cookbook project is a collection of batch processing programs intended to demonstrate how Spring Batch could be used specifically with [MarkLogic](www.marklogic.com).  Use these programs as-is or modify to accomplish your requirements.  

# Prerequisites

 * JDK 1.8
 * MarkLogic 8+
   * Default is the app server on port 8000 and the Documents database
   * See job.properties under _subproject_/src/test/resources
 
# Recipes

1) delimited - import delimited files into MarkLogic
1) tika - Use Apache Tika to extract unstructured content from binaries and ingest extracted text into MarkLogic
1) rdbms - import customer data from an H2 relational database
