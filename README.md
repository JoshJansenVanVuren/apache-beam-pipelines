# apache-beam-pipelines
Various batch and streaming apache beam pipeline implementations and examples. This README serves as a skeleton for getting the implementations to work on your own machine.

## Preliminaries
1. You need a java JDK: I used version 8 (jdk-8u231-linux-x64.tar.gz for ubuntu from java's website) this is a useful [tutorial](https://www.javahelps.com/2015/03/install-oracle-jdk-in-ubuntu.html)
2. Install Apache Maven (I used version 3.6.0)
3. It's probably useful to have done the MinimalWordCount example through Apache's tutorial on their [website](https://beam.apache.org/get-started/wordcount-example/#minimalwordcount-example) and to look through some of the Apache Documentation to get an idea of the programming model.
4. An IDE is useful, VS Code is the one I use.

## Google Cloud Preliminaries
1. You need a google cloud account.
2. Know how to use/create these:
  * Pub/Sub - Basically a cloud message queue implementation
  * DataFlow - A runner for the pipeline
  * Bucket - A cloud storage mechanism
  * BigQuery - Basically a cloud database.

## batch-working
Example of a batch pipeline
TODO...

## stream-working-pub-sub
This folder is an implementation of a streaming pipeline that is subscribed to a Pub/Sub Article whose messages get passed through the pipeline and is then output to another Pub/Sub.

### Geting the streaming Pub/Sub Working
`BioStatsPipe.java` is the main class.

`Constants.java` encompasses all the constants for the classes.

`Person.java` defines a record for the data to be passed through the pipeline

`biostats.csv` is the input file

	
1. Clone the code to your local machine.
2. Setup a Pub/Sub to listen to
3. Setup a Pub/Sub to output to
4. Use the following Maven command
```
mvn  -Pdataflow-runner compile exec:java \
      -Dexec.mainClass=org.ambrite.josh.bioStatsPipe \
      -Dexec.cleanupDaemonThreads=false \
      -Dexec.args=" \
            --project={PROJECT-ID} \
            --stagingLocation=gs://{BUCKET-ID}/staging \
            --tempLocation=gs://{BUCKET-ID}/temp \
            --runner=DataflowRunner \
            --inputTopic=projects/{PROJECT-ID}/topics/{INPUT-TOPIC} \
            --invalidOutputTopic=projects/{PROJECT-ID}/topics/{OUTPUT-TOPIC-FOR-INVALID-RECORDS} \
            --validOutputTopic=projects/{PROJECT-ID}/topics/{OUTPUT-TOPIC-FOR-VALID-RECORDS}"
```

## word-count-beam
Apache tutorial of a word count example for batch streaming
