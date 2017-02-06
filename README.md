# Hector - Ingest Delimited Files into MarkLogic

## What is Hector?

Hector is the code name for a program that will ingest a delimited text file into MarkLogic.  It is built using [MarkLogic Spring Batch](https://github.com/sastafford/marklogic-spring-batch) making it extensible.  It provides the ability of data transformation while achieving high levels of performance.  
 
 
## How do I install Hector?

1) Install the hector distribution.  The installation will be deployed under ./build/install directory.

     gradlew installDist

2) Make sure you have a job.properties file defined with MarkLogic connection configuration. 

3) Command line parameters include the following 

 * input_file_path
 * delimited_root_name
 * document_type
 * output_collections
 * thread_size (optional)
 * chunk_size (optional)

## Example

    hector.bat com.marklogic.hector.JobConfig job input_file_path=./src/test/resources/Most_Popular_Baby_Names_NYC.csv delimited_root_name=baby document_type=xml output_collections=baby
    
## Example with Transform

A transform can be defined by subclassing the XmlStringColumnMapSerializer.  See the BabyNameColumnMapSerializer for an example.  

    hector.bat com.marklogic.hector.JobConfig job input_file_path=./src/test/resources/Most_Popular_Baby_Names_NYC.csv delimited_root_name=baby document_type=xml output_collections=baby output_transform=com.marklogic.hector.BabyNameColumnMapSerializer
