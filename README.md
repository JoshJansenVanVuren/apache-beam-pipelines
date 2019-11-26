# apache-beam-pipelines

Under Development...

Various batch and streaming apache beam pipeline implementations and examples.

# Preliminaries
1. You need a java JDK: I used version 8 (jdk-8u231-linux-x64.tar.gz for ubuntu from java's website) this is a useful [tutorial](https://www.javahelps.com/2015/03/install-oracle-jdk-in-ubuntu.html)
2. Install Apache Maven (I used version 3.6.0)
3. It's probably useful to have done the MinimalWordCount example through Apache's tutorial on their [website](https://beam.apache.org/get-started/wordcount-example/#minimalwordcount-example)

## batch-working
Example of a batch pipeline
TODO...

## stream-working-pub-sub
This folder is an implementation of a streaming pipeline that is subscribed to a Pub/Sub Article whose messages get passed through the pipeline and is then output to another Pub/Sub.

### Geting the streaming Pub/Sub Working
`BioStatsPipe.java` is the main class.
`Constants.java` encompasses all the constants for the classes.
`Person.java` defines a record for the data to be passed through the pipeline



## word-count-beam
Apache tutorial of a word count example for batch streaming
